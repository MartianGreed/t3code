// @effect-diagnostics-next-line nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationThread,
  type OrchestrationThreadShell,
  type OrchestrationProjectShell,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { afterEach, describe, expect, it } from "@effect/vitest";
import * as Config from "../config.ts";
import { ProcessRunner, type ProcessRunInput } from "../processRunner.ts";
import * as ProjectLoader from "../project/T3ProjectFileLoader.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as Exos from "./ExosIntegration.ts";

const roots: string[] = [];
afterEach(async () => {
  for (const root of roots.splice(0)) await NodeFSP.rm(root, { recursive: true, force: true });
});
const now = "2026-09-15T12:00:00.000Z";
const id = ThreadId.make("t3-real-thread");
const projectId = ProjectId.make("project-id");
async function harness() {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-exos-test-"));
  roots.push(root);
  const workspace = NodePath.join(root, "product");
  const repository = NodePath.join(root, "demo-os");
  const skills = NodePath.join(root, "skills");
  const bundle = NodePath.join(repository, "knowledge/workstreams/artefacts/delivery");
  await NodeFSP.mkdir(workspace, { recursive: true });
  await NodeFSP.mkdir(bundle, { recursive: true });
  await NodeFSP.writeFile(NodePath.join(repository, "manifest.yaml"), "project: demo\n");
  await NodeFSP.writeFile(
    NodePath.join(repository, "knowledge/workstreams/delivery.md"),
    "# Delivery\n",
  );
  await NodeFSP.mkdir(NodePath.join(skills, "workstream/references"), { recursive: true });
  await NodeFSP.writeFile(
    NodePath.join(skills, "workstream/SKILL.md"),
    "# Current upstream workstream skill\n",
  );
  await NodeFSP.writeFile(NodePath.join(skills, "workstream/references/roles.md"), "Role guide\n");
  const link = { project: "demo", repository: "../demo-os", skillsDirectory: "../skills" };
  const writeLink = (exos: typeof link | undefined) =>
    NodeFSP.writeFile(NodePath.join(workspace, "t3.json"), JSON.stringify(exos ? { exos } : {}));
  await writeLink(link);
  const shell: OrchestrationThreadShell = {
    id,
    projectId,
    title: "Delivery",
    modelSelection: { instanceId: ProviderInstanceId.make("claudeCode"), model: "test" },
    runtimeMode: "approval-required",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    pullRequests: [],
    latestTurn: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    session: null,
    latestUserMessageAt: now,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
  };
  let thread: OrchestrationThread = {
    ...shell,
    deletedAt: null,
    messages: [
      {
        id: MessageId.make("message"),
        role: "user",
        text: "Build the requested feature",
        turnId: null,
        streaming: false,
        createdAt: now,
        updatedAt: now,
      },
    ],
    activities: [],
    checkpoints: [],
    proposedPlans: [],
  };
  const project: OrchestrationProjectShell = {
    id: projectId,
    title: "Demo",
    workspaceRoot: workspace,
    defaultModelSelection: null,
    scripts: [],
    createdAt: now,
    updatedAt: now,
  };
  const calls: ProcessRunInput[] = [];
  let fail = false;
  let echo = id;
  const dependencies = Layer.mergeAll(
    Layer.mock(ProjectionSnapshotQuery)({
      getThreadShellById: (threadId) =>
        Effect.succeed(threadId === id ? Option.some(shell) : Option.none()),
      getProjectShellById: () => Effect.succeed(Option.some(project)),
      getThreadDetailSnapshot: () => Effect.succeed(Option.some({ snapshotSequence: 1, thread })),
    }),
    Layer.succeed(ProcessRunner, {
      run: (input) =>
        Effect.sync(() => {
          calls.push(input);
          return {
            // @effect-diagnostics-next-line preferSchemaOverJson:off
            stdout: JSON.stringify({
              success: !fail,
              data: {
                projectSlug: "demo",
                workstream: {
                  stem: "delivery",
                  sessions: [{ hostSessionId: echo, role: "implementer", agent: "t3-code" }],
                },
              },
            }),
            stderr: fail ? "private error, must not leak" : "",
            code: null,
            timedOut: false,
            stdoutTruncated: false,
            stderrTruncated: false,
            stdoutInvalidUtf8: false,
            stderrInvalidUtf8: false,
          };
        }).pipe(
          Effect.map((result) => ({
            ...result,
            code: (fail ? 1 : 0) as import("effect/unstable/process/ChildProcessSpawner").ExitCode,
          })),
        ),
    }),
    ProjectLoader.layer,
    Config.layerTest(workspace, NodePath.join(root, "state")),
  ).pipe(Layer.provide(NodeServices.layer));
  const create = () => Exos.make.pipe(Effect.provide(dependencies));
  return {
    root,
    workspace,
    repository,
    skills,
    bundle,
    link,
    writeLink,
    calls,
    setText: (text: string) => {
      thread = { ...thread, messages: [{ ...thread.messages[0]!, text }] };
    },
    create,
    setFail: (value: boolean) => {
      fail = value;
    },
    setEcho: (value: ThreadId) => {
      echo = value;
    },
  };
}
const reject = <A>(effect: Effect.Effect<A, Exos.ExosError>, message: string) =>
  effect.pipe(
    Effect.flip,
    Effect.tap((error) => Effect.sync(() => expect(error.message).toContain(message))),
  );
const promise = Effect.promise;

describe("native Exos integration", () => {
  it.effect(
    "registers the real thread and role once, captures archive-readable text outside the bundle, and restores after restart",
    () =>
      Effect.gen(function* () {
        const h = yield* promise(harness);
        const exos = yield* h.create();
        const bound = yield* exos.bind(id, "delivery", "implementer");
        expect(bound.registered).toBe(true);
        expect(h.calls[0]?.args).toEqual([
          "workstream",
          "register-session",
          "demo",
          "delivery",
          "--session",
          id,
          "--agent",
          "t3-code",
          "--role",
          "implementer",
          "--json",
        ]);
        expect(h.calls[0]?.timeout).toBe("15 seconds");
        expect(yield* promise(() => NodeFSP.readdir(h.bundle))).toEqual([]);
        const text = yield* promise(() => NodeFSP.readFile(bound.capturePath!, "utf8"));
        const records = text
          .trim()
          .split("\n")
          .map((line) => JSON.parse(line));
        expect(records[0]).toMatchObject({
          source: "other",
          host_session_id: id,
          role: "implementer",
        });
        expect(records[1]).toMatchObject({
          type: "user",
          message: { content: "Build the requested feature" },
        });
        expect((yield* promise(() => NodeFSP.stat(bound.capturePath!))).mode & 0o777).toBe(0o600);
        expect(
          (yield* promise(() => NodeFSP.stat(NodePath.dirname(bound.capturePath!)))).mode & 0o777,
        ).toBe(0o700);
        const restarted = yield* h.create();
        expect(yield* restarted.bindings()).toEqual([id]);
        expect((yield* restarted.sync(id))?.registered).toBe(true);
        expect(h.calls).toHaveLength(1);
      }),
  );
  it.effect(
    "retains failed registration for explicit retry and recovery without leaking CLI output",
    () =>
      Effect.gen(function* () {
        const h = yield* promise(harness);
        h.setFail(true);
        const exos = yield* h.create();
        const failed = yield* exos.bind(id, "delivery", "implementer");
        expect(failed.registered).toBe(false);
        expect(failed.registrationError).toContain("exos_sync");
        expect(failed.registrationError).not.toContain("private error");
        expect(failed.capturePath).not.toBeNull();
        h.setFail(false);
        const restarted = yield* h.create();
        expect((yield* restarted.sync(id))?.registered).toBe(true);
        expect((yield* restarted.status(id)).binding?.registrationError).toBeNull();
      }),
  );
  it.effect("rejects a success response for another host session", () =>
    Effect.gen(function* () {
      const h = yield* promise(harness);
      h.setEcho(ThreadId.make("other-thread"));
      const exos = yield* h.create();
      expect((yield* exos.bind(id, "delivery", "implementer")).registered).toBe(false);
    }),
  );
  it.effect(
    "refuses disabled projects, missing threads, mismatched manifests and changed roles",
    () =>
      Effect.gen(function* () {
        const h = yield* promise(harness);
        const exos = yield* h.create();
        yield* reject(
          exos.bind(ThreadId.make("other"), "delivery", "implementer"),
          "thread is unavailable",
        );
        yield* promise(() => h.writeLink(undefined));
        expect((yield* exos.status(id)).enabled).toBe(false);
        yield* reject(exos.bind(id, "delivery", "implementer"), "not enabled");
        yield* promise(() => h.writeLink(h.link));
        yield* promise(() =>
          NodeFSP.writeFile(NodePath.join(h.repository, "manifest.yaml"), "project: another\n"),
        );
        yield* reject(exos.bind(id, "delivery", "implementer"), "manifest");
        expect(h.calls).toHaveLength(0);
        yield* promise(() =>
          NodeFSP.writeFile(NodePath.join(h.repository, "manifest.yaml"), "project: demo\n"),
        );
        yield* exos.bind(id, "delivery", "implementer");
        yield* reject(exos.bind(id, "delivery", "verifier"), "immutable");
      }),
  );
  it.effect("detaches and resumes without closing a workstream or registering twice", () =>
    Effect.gen(function* () {
      const h = yield* promise(harness);
      const exos = yield* h.create();
      const original = yield* exos.bind(id, "delivery", "implementer");
      yield* exos.detach(id);
      h.setText("New text");
      expect((yield* exos.sync(id))?.active).toBe(false);
      expect(yield* promise(() => NodeFSP.readFile(original.capturePath!, "utf8"))).not.toContain(
        "New text",
      );
      expect(yield* exos.bindings()).toEqual([]);
      expect((yield* exos.bind(id, "delivery", "implementer")).active).toBe(true);
      expect(yield* promise(() => NodeFSP.readFile(original.capturePath!, "utf8"))).toContain(
        "New text",
      );
      expect(h.calls).toHaveLength(1);
    }),
  );
  it.effect("removing the project link stops capture even if a binding exists", () =>
    Effect.gen(function* () {
      const h = yield* promise(harness);
      const exos = yield* h.create();
      const bound = yield* exos.bind(id, "delivery", "implementer");
      yield* promise(() => h.writeLink(undefined));
      h.setText("After disabling");
      yield* exos.sync(id);
      expect(yield* promise(() => NodeFSP.readFile(bound.capturePath!, "utf8"))).not.toContain(
        "After disabling",
      );
    }),
  );
  it.effect("discovers live skills and confines referenced files to their skill directory", () =>
    Effect.gen(function* () {
      const h = yield* promise(harness);
      const exos = yield* h.create();
      expect(yield* exos.skills(id)).toEqual(["workstream"]);
      expect(yield* exos.readSkill(id, "workstream")).toContain("Current upstream");
      expect(yield* exos.readSkill(id, "workstream", "references/roles.md")).toBe("Role guide\n");
      yield* reject(exos.readSkill(id, "workstream", "../../demo-os/manifest.yaml"), "outside");
      yield* promise(() =>
        NodeFSP.symlink(
          NodePath.join(h.repository, "manifest.yaml"),
          NodePath.join(h.skills, "workstream/escape.md"),
        ),
      );
      yield* reject(exos.readSkill(id, "workstream", "escape.md"), "outside");
    }),
  );
  it.effect("retains the previous capture and reports oversized transcripts", () =>
    Effect.gen(function* () {
      const h = yield* promise(harness);
      const exos = yield* h.create();
      const bound = yield* exos.bind(id, "delivery", "implementer");
      h.setText("x".repeat(17 * 1024 * 1024));
      const result = yield* exos.sync(id);
      expect(result?.captureError).toContain("16 MiB");
      expect((yield* promise(() => NodeFSP.stat(bound.capturePath!))).size).toBeLessThan(10_000);
    }),
  );
  it.effect("serializes concurrent bind attempts", () =>
    Effect.gen(function* () {
      const h = yield* promise(harness);
      const exos = yield* h.create();
      yield* Effect.all(
        [exos.bind(id, "delivery", "implementer"), exos.bind(id, "delivery", "implementer")],
        { concurrency: "unbounded" },
      );
      expect(h.calls).toHaveLength(1);
    }),
  );
  it.effect(
    "reports background configuration failures and refuses redirected capture directories",
    () =>
      Effect.gen(function* () {
        const h = yield* promise(harness);
        const exos = yield* h.create();
        const bound = yield* exos.bind(id, "delivery", "implementer");
        yield* promise(() =>
          NodeFSP.writeFile(NodePath.join(h.repository, "manifest.yaml"), "project: changed\n"),
        );
        expect((yield* exos.sync(id))?.captureError).toContain("manifest");
        expect((yield* exos.status(id)).binding?.captureError).toContain("manifest");
        yield* promise(() =>
          NodeFSP.writeFile(NodePath.join(h.repository, "manifest.yaml"), "project: demo\n"),
        );
        const captures = NodePath.dirname(bound.capturePath!);
        const outside = NodePath.join(h.root, "outside");
        yield* promise(() => NodeFSP.mkdir(outside));
        yield* promise(() => NodeFSP.rename(captures, `${captures}-original`));
        yield* promise(() => NodeFSP.symlink(outside, captures));
        expect((yield* exos.sync(id))?.captureError).toContain("private Exos capture");
        expect(yield* promise(() => NodeFSP.readdir(outside))).toEqual([]);
      }),
  );
});
