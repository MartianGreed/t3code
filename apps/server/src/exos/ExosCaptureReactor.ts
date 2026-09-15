import type { OrchestrationEvent, ThreadId } from "@t3tools/contracts";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { forkParked } from "../serverActivation.ts";
import { ExosIntegration } from "./ExosIntegration.ts";

export class ExosCaptureReactor extends Context.Service<
  ExosCaptureReactor,
  {
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
    readonly drainThrough: (sequence: number) => Effect.Effect<void>;
  }
>()("t3/exos/ExosCaptureReactor") {}

export function captureThread(event: OrchestrationEvent): ThreadId | undefined {
  switch (event.type) {
    case "thread.message-sent":
      return event.payload.streaming ? undefined : event.payload.threadId;
    case "thread.session-set":
    case "thread.turn-diff-completed":
    case "thread.archived":
      return event.payload.threadId;
    default:
      return undefined;
  }
}

export const make = Effect.gen(function* () {
  const engine = yield* OrchestrationEngineService;
  const exos = yield* ExosIntegration;
  const seen = yield* SubscriptionRef.make(0);
  const pending = new Set<ThreadId>();
  const dirty = new Set<ThreadId>();
  const sync = (threadId: ThreadId) =>
    exos
      .sync(threadId)
      .pipe(
        Effect.catch((error) =>
          Effect.logWarning("exos.capture.failed", { threadId, message: error.message }),
        ),
      );
  const worker = yield* makeDrainableWorker<ThreadId, never, never>((threadId) =>
    Effect.gen(function* () {
      do {
        dirty.delete(threadId);
        yield* sync(threadId);
      } while (dirty.has(threadId));
      pending.delete(threadId);
    }),
  );
  const enqueue = Effect.fn("ExosCaptureReactor.enqueue")(function* (threadId: ThreadId) {
    if (pending.has(threadId)) {
      dirty.add(threadId);
      return;
    }
    // Coalesce bursts per thread and bound the backlog. A later event or startup
    // reconciliation retries omitted work from T3's durable projection.
    if (pending.size >= 1024) {
      yield* Effect.logWarning("exos.capture.backlog_full", { threadId });
      return;
    }
    pending.add(threadId);
    yield* worker.enqueue(threadId);
  });
  const noteSeen = (sequence: number) =>
    SubscriptionRef.update(seen, (previous) => Math.max(previous, sequence));
  return ExosCaptureReactor.of({
    start: Effect.fn("ExosCaptureReactor.start")(function* () {
      const events = yield* engine.subscribeDomainEvents;
      yield* forkParked(
        Stream.runForEach(
          events.pipe(
            Stream.onStart(
              Effect.gen(function* () {
                yield* engine.latestSequence.pipe(Effect.flatMap(noteSeen));
                const bindings = yield* exos
                  .bindings()
                  .pipe(
                    Effect.catch((error) =>
                      Effect.logWarning("exos.recovery.failed", { message: error.message }).pipe(
                        Effect.as([]),
                      ),
                    ),
                  );
                yield* Effect.forEach(bindings, enqueue, { discard: true });
              }),
            ),
          ),
          (event) =>
            Effect.gen(function* () {
              const threadId = captureThread(event);
              if (threadId) yield* enqueue(threadId);
              yield* noteSeen(event.sequence);
            }),
        ),
      );
    }),
    drainThrough: (sequence) =>
      SubscriptionRef.changes(seen).pipe(
        Stream.filter((value) => value >= sequence),
        Stream.runHead,
        Effect.andThen(worker.drain),
      ),
  });
});
export const layer = Layer.effect(ExosCaptureReactor, make);
