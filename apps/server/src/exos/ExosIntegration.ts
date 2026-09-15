import * as NodeCrypto from "node:crypto";
// Descriptor-based bounded reads and atomic rename keep transcript files private.
// @effect-diagnostics-next-line nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off
import * as NodePath from "node:path";
import { ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import { parse as parseYaml } from "yaml";
import * as ServerConfig from "../config.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as T3ProjectFileLoader from "../project/T3ProjectFileLoader.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";

export const ExosRole = Schema.Literals([
  "coordination",
  "orchestrator",
  "implementer",
  "verifier",
  "publisher",
]);
export const ExosName = Schema.String.check(Schema.isPattern(/^[a-z0-9][a-z0-9-]{0,149}$/));
export const ExosBinding = Schema.Struct({
  threadId: ThreadId,
  project: Schema.String,
  repository: Schema.String,
  workstream: ExosName,
  role: ExosRole,
  active: Schema.Boolean,
  registered: Schema.Boolean,
  registrationError: Schema.NullOr(Schema.String),
  captureError: Schema.NullOr(Schema.String),
  capturePath: Schema.NullOr(Schema.String),
  capturedAt: Schema.NullOr(Schema.String),
});
export type ExosBinding = typeof ExosBinding.Type;
export const ExosStatus = Schema.Struct({
  enabled: Schema.Boolean,
  threadId: ThreadId,
  project: Schema.NullOr(Schema.String),
  binding: Schema.NullOr(ExosBinding),
});
export class ExosError extends Schema.TaggedError<ExosError>()("ExosError", {
  message: Schema.String,
}) {}
const failure = (message: string) => new ExosError({ message });
const io = <A>(message: string, run: () => Promise<A>) =>
  Effect.tryPromise({ try: run, catch: () => failure(message) });
const decodeName = Schema.decodeUnknownSync(ExosName);
const decodeBinding = Schema.decodeUnknownSync(Schema.fromJsonString(ExosBinding));
const decodeManifest = Schema.decodeUnknownSync(Schema.Struct({ project: Schema.String }));
const decodeSuccess = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      success: Schema.Literal(true),
      data: Schema.Struct({
        projectSlug: Schema.String,
        workstream: Schema.Struct({
          stem: Schema.String,
          sessions: Schema.Array(
            Schema.Struct({
              hostSessionId: Schema.String,
              agent: Schema.String,
              role: Schema.optional(ExosRole),
            }),
          ),
        }),
      }),
    }),
  ),
);
const fileKey = (threadId: string) =>
  NodeCrypto.createHash("sha256").update(threadId).digest("hex");
const isInside = (root: string, target: string) => {
  const relative = NodePath.relative(root, target);
  return (
    relative !== ".." && !relative.startsWith(`..${NodePath.sep}`) && !NodePath.isAbsolute(relative)
  );
};

/** Private integration state is separate from both T3's projection and the project OS bundle. */
async function privateDirectory(directory: string) {
  await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
  const stat = await NodeFSP.lstat(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error("Unsafe state directory");
  await NodeFSP.chmod(directory, 0o700);
}
async function atomicWrite(file: string, contents: string) {
  await privateDirectory(NodePath.dirname(file));
  const temporary = `${file}.${NodeCrypto.randomUUID()}.tmp`;
  try {
    await NodeFSP.writeFile(temporary, contents, { mode: 0o600, flag: "wx" });
    await NodeFSP.rename(temporary, file);
  } finally {
    await NodeFSP.rm(temporary, { force: true });
  }
}
async function readBounded(file: string, maxBytes: number) {
  const handle = await NodeFSP.open(
    file,
    NodeFSP.constants.O_RDONLY | (NodeFSP.constants.O_NOFOLLOW ?? 0),
  );
  try {
    const stat = await handle.stat();
    if (!stat.isFile() || stat.size > maxBytes) throw new Error("File exceeds limit");
    const buffer = Buffer.alloc(maxBytes + 1);
    let total = 0;
    while (total < buffer.length) {
      const { bytesRead } = await handle.read(buffer, total, buffer.length - total, total);
      if (bytesRead === 0) break;
      total += bytesRead;
    }
    if (total > maxBytes) throw new Error("File exceeds limit");
    return buffer.subarray(0, total).toString("utf8");
  } finally {
    await handle.close();
  }
}

export class ExosIntegration extends Context.Service<
  ExosIntegration,
  {
    readonly status: (threadId: ThreadId) => Effect.Effect<typeof ExosStatus.Type, ExosError>;
    readonly bind: (
      threadId: ThreadId,
      workstream: string,
      role: typeof ExosRole.Type,
    ) => Effect.Effect<ExosBinding, ExosError>;
    readonly detach: (threadId: ThreadId) => Effect.Effect<ExosBinding, ExosError>;
    readonly sync: (threadId: ThreadId) => Effect.Effect<ExosBinding | null, ExosError>;
    readonly bindings: () => Effect.Effect<ReadonlyArray<ThreadId>, ExosError>;
    readonly skills: (threadId: ThreadId) => Effect.Effect<ReadonlyArray<string>, ExosError>;
    readonly readSkill: (
      threadId: ThreadId,
      name: string,
      file?: string,
    ) => Effect.Effect<string, ExosError>;
  }
>()("t3/exos/ExosIntegration") {}

export const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const projects = yield* T3ProjectFileLoader.T3ProjectFileLoader;
  const runner = yield* ProcessRunner.ProcessRunner;
  const lock = yield* Semaphore.make(1);
  const root = NodePath.join(config.stateDir, "exos");
  const bindingPath = (threadId: ThreadId) =>
    NodePath.join(root, "bindings", `${fileKey(threadId)}.json`);
  const initialize = io("Could not initialize private Exos state.", () => privateDirectory(root));
  const load = (threadId: ThreadId) =>
    io("Could not read Exos binding.", async () => {
      try {
        const binding = decodeBinding(await readBounded(bindingPath(threadId), 16_384));
        if (binding.threadId !== threadId) throw new Error("Binding mismatch");
        return binding;
      } catch (error) {
        if (error instanceof Error && "code" in error && error.code === "ENOENT") return null;
        throw error;
      }
    });
  const save = (binding: ExosBinding) =>
    initialize.pipe(
      Effect.andThen(
        io("Could not save Exos binding.", () =>
          atomicWrite(bindingPath(binding.threadId), JSON.stringify(binding)),
        ),
      ),
      Effect.as(binding),
    );
  const projectOf = Effect.fn("ExosIntegration.projectOf")(function* (threadId: ThreadId) {
    const thread = yield* snapshots
      .getThreadShellById(threadId)
      .pipe(Effect.mapError(() => failure("Could not read T3 thread.")));
    if (Option.isNone(thread)) return yield* failure("T3 thread is unavailable.");
    const project = yield* snapshots
      .getProjectShellById(thread.value.projectId)
      .pipe(Effect.mapError(() => failure("Could not read T3 project.")));
    if (Option.isNone(project)) return yield* failure("T3 project is unavailable.");
    const file = yield* projects.load(project.value.workspaceRoot);
    return {
      workspaceRoot: project.value.workspaceRoot,
      exos: Option.isSome(file) ? file.value.exos : undefined,
    };
  });
  const context = Effect.fn("ExosIntegration.context")(function* (threadId: ThreadId) {
    const project = yield* projectOf(threadId);
    if (!project.exos)
      return yield* failure(
        "Exos is not enabled. Add an exos project link to t3.json on the T3 server.",
      );
    const settings = project.exos;
    const repository = yield* io(
      "Exos repository is unavailable or its manifest does not match the configured project.",
      async () => {
        const repository = await NodeFSP.realpath(
          NodePath.resolve(project.workspaceRoot, settings.repository),
        );
        const manifest = decodeManifest(
          parseYaml(await readBounded(NodePath.join(repository, "manifest.yaml"), 262_144)),
        );
        if (manifest.project !== settings.project) throw new Error("Project mismatch");
        const realState = await NodeFSP.realpath(config.stateDir);
        if (isInside(repository, realState))
          throw new Error("Capture state must be outside project OS repository");
        return repository;
      },
    );
    return { ...project, settings, repository };
  });
  const verifyBinding = (
    binding: ExosBinding,
    project: { repository: string; settings: { project: string } },
  ) =>
    binding.project === project.settings.project && binding.repository === project.repository
      ? Effect.void
      : Effect.fail(
          failure(
            "The project link changed. Restore the original link before resuming this thread's binding.",
          ),
        );
  const verifyWorkstream = (repository: string, workstream: string) =>
    io(
      "The workstream record or artefact directory is missing or outside the project OS clone.",
      async () => {
        decodeName(workstream);
        const record = await NodeFSP.realpath(
          NodePath.join(repository, "knowledge/workstreams", `${workstream}.md`),
        );
        const bundle = await NodeFSP.realpath(
          NodePath.join(repository, "knowledge/workstreams/artefacts", workstream),
        );
        if (
          !isInside(repository, record) ||
          !isInside(repository, bundle) ||
          !(await NodeFSP.stat(bundle)).isDirectory()
        )
          throw new Error("Unsafe workstream");
      },
    );
  const register = Effect.fn("ExosIntegration.register")(function* (binding: ExosBinding) {
    if (binding.registered) return binding;
    const result = yield* runner
      .run({
        command: "exos",
        args: [
          "workstream",
          "register-session",
          binding.project,
          binding.workstream,
          "--session",
          binding.threadId,
          "--agent",
          "t3-code",
          "--role",
          binding.role,
          "--json",
        ],
        timeout: "15 seconds",
        maxOutputBytes: 262_144,
      })
      .pipe(
        Effect.flatMap((result) =>
          result.code === 0 && !result.timedOut
            ? Effect.try({
                try: () => {
                  const { data } = decodeSuccess(result.stdout);
                  if (
                    data.projectSlug !== binding.project ||
                    data.workstream.stem !== binding.workstream ||
                    !data.workstream.sessions.some(
                      (session) =>
                        session.hostSessionId === binding.threadId &&
                        session.agent === "t3-code" &&
                        session.role === binding.role,
                    )
                  )
                    throw new Error("Registration mismatch");
                  return data;
                },
                catch: () =>
                  failure("Exos did not confirm registration. Check CLI compatibility and retry."),
              })
            : Effect.fail(
                failure(
                  "Exos registration failed. Check CLI authentication and workstream state, then retry.",
                ),
              ),
        ),
        Effect.map(() => ({ ...binding, registered: true, registrationError: null })),
        Effect.catch(() =>
          Effect.succeed({
            ...binding,
            registered: false,
            registrationError:
              "Exos registration failed. Check the installed exos CLI, authentication, and workstream state; run exos_sync to retry.",
          }),
        ),
      );
    return yield* save(result);
  });
  const capture = Effect.fn("ExosIntegration.capture")(function* (binding: ExosBinding) {
    const snapshot = yield* snapshots
      .getThreadDetailSnapshot(binding.threadId)
      .pipe(Effect.mapError(() => failure("Could not read T3 transcript.")));
    if (Option.isNone(snapshot)) return yield* failure("T3 transcript is unavailable.");
    const thread = snapshot.value.thread;
    const path = NodePath.join(root, "captures", `${fileKey(binding.threadId)}.jsonl`);
    const records = [
      {
        type: "metadata",
        source: "other",
        host_session_id: binding.threadId,
        threadId: binding.threadId,
        project: binding.project,
        workstream: binding.workstream,
        role: binding.role,
        provider: thread.session?.providerName ?? null,
      },
      ...thread.messages.map((message) => ({
        type: message.role,
        timestamp: message.createdAt,
        id: message.id,
        turnId: message.turnId,
        streaming: message.streaming,
        message: { content: message.text },
        attachments: message.attachments,
      })),
      ...thread.proposedPlans.map((plan) => ({ type: "plan", ...plan })),
      ...thread.activities.map((activity) => ({ type: "activity", ...activity })),
    ];
    const text = records.map((record) => JSON.stringify(record)).join("\n") + "\n";
    if (Buffer.byteLength(text) > 16 * 1024 * 1024)
      return yield* failure(
        "Transcript exceeds the 16 MiB capture limit. The previous capture was retained.",
      );
    yield* initialize;
    yield* io("Could not write private Exos capture.", () => atomicWrite(path, text));
    return yield* save({
      ...binding,
      capturePath: path,
      capturedAt: DateTime.formatIso(yield* DateTime.now),
      captureError: null,
    });
  });
  const sync = Effect.fn("ExosIntegration.sync")(function* (threadId: ThreadId) {
    const initial = yield* load(threadId);
    if (!initial || !initial.active) return initial;
    let current = initial;
    return yield* Effect.gen(function* () {
      const project = yield* projectOf(threadId);
      if (!project.exos) return current;
      const resolved = yield* context(threadId);
      yield* verifyBinding(current, resolved);
      yield* verifyWorkstream(current.repository, current.workstream);
      current = yield* register(current);
      return yield* capture(current);
    }).pipe(Effect.catch((error) => save({ ...current, captureError: error.message })));
  });
  const skillsRoot = Effect.fn("ExosIntegration.skillsRoot")(function* (threadId: ThreadId) {
    const project = yield* context(threadId);
    if (!project.settings.skillsDirectory)
      return yield* failure(
        "Set exos.skillsDirectory in t3.json to the installed Exos skills directory on the server.",
      );
    return yield* io("Exos skills directory is unavailable.", () =>
      NodeFSP.realpath(NodePath.resolve(project.workspaceRoot, project.settings.skillsDirectory!)),
    );
  });
  return ExosIntegration.of({
    status: Effect.fn("ExosIntegration.status")(function* (threadId) {
      const project = yield* projectOf(threadId);
      return {
        enabled: project.exos !== undefined,
        threadId,
        project: project.exos?.project ?? null,
        binding: yield* load(threadId),
      };
    }),
    bind: (threadId, workstream, role) =>
      lock.withPermit(
        Effect.gen(function* () {
          const project = yield* context(threadId);
          yield* verifyWorkstream(project.repository, workstream);
          const old = yield* load(threadId);
          if (old) {
            yield* verifyBinding(old, project);
            if (old.workstream !== workstream || old.role !== role)
              return yield* failure(
                "A thread's workstream and role are immutable. Start a new thread for another assignment.",
              );
          }
          yield* save(
            old
              ? { ...old, active: true }
              : {
                  threadId,
                  project: project.settings.project,
                  repository: project.repository,
                  workstream,
                  role,
                  active: true,
                  registered: false,
                  registrationError: null,
                  captureError: null,
                  capturePath: null,
                  capturedAt: null,
                },
          );
          const binding = yield* sync(threadId);
          return binding!;
        }),
      ),
    detach: (threadId) =>
      lock.withPermit(
        Effect.gen(function* () {
          const binding = yield* load(threadId);
          if (!binding) return yield* failure("This thread has no Exos binding.");
          return yield* save({ ...binding, active: false });
        }),
      ),
    sync: (threadId) => lock.withPermit(sync(threadId)),
    bindings: () =>
      io("Could not list Exos bindings.", async () => {
        let files: string[];
        try {
          files = await NodeFSP.readdir(NodePath.join(root, "bindings"));
        } catch (error) {
          if (error instanceof Error && "code" in error && error.code === "ENOENT") return [];
          throw error;
        }
        if (files.length > 10_000) throw new Error("Too many bindings");
        const ids: ThreadId[] = [];
        for (const name of files.filter((name) => /^[a-f0-9]{64}\.json$/.test(name))) {
          const binding = decodeBinding(
            await readBounded(NodePath.join(root, "bindings", name), 16_384),
          );
          if (`${fileKey(binding.threadId)}.json` !== name) throw new Error("Binding mismatch");
          if (binding.active) ids.push(binding.threadId);
        }
        return ids;
      }),
    skills: Effect.fn("ExosIntegration.skills")(function* (threadId) {
      const root = yield* skillsRoot(threadId);
      return yield* io("Could not list Exos skills.", async () => {
        const entries = await NodeFSP.readdir(root, { withFileTypes: true });
        if (entries.length > 500) throw new Error("Too many skills");
        const names: string[] = [];
        for (const entry of entries) {
          if (!entry.isDirectory() || !/^[a-z0-9][a-z0-9-]*$/.test(entry.name)) continue;
          try {
            if ((await NodeFSP.stat(NodePath.join(root, entry.name, "SKILL.md"))).isFile())
              names.push(entry.name);
          } catch (error) {
            if (!(error instanceof Error && "code" in error && error.code === "ENOENT"))
              throw error;
          }
        }
        return names.sort();
      });
    }),
    readSkill: Effect.fn("ExosIntegration.readSkill")(function* (
      threadId,
      name,
      file = "SKILL.md",
    ) {
      const root = yield* skillsRoot(threadId);
      return yield* io(
        "Skill file is unavailable, outside its skill directory, or exceeds 64 KiB.",
        async () => {
          decodeName(name);
          const directory = await NodeFSP.realpath(NodePath.join(root, name));
          const target = await NodeFSP.realpath(NodePath.resolve(directory, file));
          if (!isInside(root, directory) || !isInside(directory, target))
            throw new Error("Outside skill");
          return readBounded(target, 65_536);
        },
      );
    }),
  });
});
export const layer = Layer.effect(ExosIntegration, make);
