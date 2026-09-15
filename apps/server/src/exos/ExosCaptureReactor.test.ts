import { describe, expect, it } from "@effect/vitest";
import { EventId, ThreadId, type OrchestrationEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { ExosIntegration } from "./ExosIntegration.ts";
import * as Reactor from "./ExosCaptureReactor.ts";

const threadId = ThreadId.make("capture-thread");
const now = "2026-09-15T12:00:00.000Z";
function event(sequence: number, streaming = false): OrchestrationEvent {
  return {
    sequence,
    eventId: EventId.make(`event-${sequence}`),
    aggregateKind: "thread",
    aggregateId: threadId,
    occurredAt: now,
    commandId: null,
    correlationId: null,
    causationEventId: null,
    metadata: {},
    type: "thread.message-sent",
    payload: {
      threadId,
      messageId: `message-${sequence}` as import("@t3tools/contracts").MessageId,
      role: "assistant",
      text: "Update",
      turnId: null,
      streaming,
      createdAt: now,
      updatedAt: now,
    },
  };
}

describe("Exos capture lifecycle", () => {
  it.effect("recovers bindings, ignores streaming deltas, and drains completed messages", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* PubSub.unbounded<OrchestrationEvent>();
        const subscription = yield* PubSub.subscribe(bus);
        const recovered = yield* Deferred.make<void>();
        const calls: ThreadId[] = [];
        const reactor = yield* Reactor.make.pipe(
          Effect.provide(
            Layer.mergeAll(
              Layer.mock(OrchestrationEngineService)({
                subscribeDomainEvents: Effect.succeed(Stream.fromSubscription(subscription)),
                latestSequence: Effect.succeed(0),
              }),
              Layer.mock(ExosIntegration)({
                bindings: () => Effect.succeed([threadId]),
                sync: (id) =>
                  Effect.sync(() => {
                    calls.push(id);
                    return null;
                  }).pipe(Effect.tap(() => Deferred.succeed(recovered, undefined))),
              }),
            ),
          ),
        );
        yield* reactor.start();
        yield* Deferred.await(recovered);
        yield* PubSub.publish(bus, event(1, true));
        yield* reactor.drainThrough(1);
        expect(calls).toEqual([threadId]);
        yield* PubSub.publish(bus, event(2));
        yield* reactor.drainThrough(2);
        expect(calls).toEqual([threadId, threadId]);
      }),
    ),
  );
  it.effect("coalesces a burst while a capture is in flight without losing the final update", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* PubSub.unbounded<OrchestrationEvent>();
        const subscription = yield* PubSub.subscribe(bus);
        const firstStarted = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let captures = 0;
        const reactor = yield* Reactor.make.pipe(
          Effect.provide(
            Layer.mergeAll(
              Layer.mock(OrchestrationEngineService)({
                subscribeDomainEvents: Effect.succeed(Stream.fromSubscription(subscription)),
                latestSequence: Effect.succeed(0),
              }),
              Layer.mock(ExosIntegration)({
                bindings: () => Effect.succeed([]),
                sync: () =>
                  Effect.gen(function* () {
                    captures++;
                    if (captures === 1) {
                      yield* Deferred.succeed(firstStarted, undefined);
                      yield* Deferred.await(release);
                    }
                    return null;
                  }),
              }),
            ),
          ),
        );
        yield* reactor.start();
        yield* PubSub.publish(bus, event(1));
        yield* Deferred.await(firstStarted);
        for (let i = 2; i <= 20; i++) yield* PubSub.publish(bus, event(i));
        yield* Deferred.succeed(release, undefined);
        yield* reactor.drainThrough(20);
        expect(captures).toBeGreaterThanOrEqual(2);
        expect(captures).toBeLessThan(20);
      }),
    ),
  );
});
