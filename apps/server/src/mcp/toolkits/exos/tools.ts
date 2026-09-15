import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";
import {
  ExosBinding,
  ExosError,
  ExosIntegration,
  ExosName,
  ExosRole,
  ExosStatus,
} from "../../../exos/ExosIntegration.ts";
import { McpInvocationContext } from "../../McpInvocationContext.ts";

const dependencies = [McpInvocationContext, ExosIntegration];
export const ExosToolkit = Toolkit.make(
  Tool.make("exos_status", {
    description:
      "Read this thread's Exos project link, workstream assignment, registration and capture status. Available when the project has an exos entry in t3.json. Does not load project context or start a workstream.",
    success: ExosStatus,
    failure: ExosError,
    dependencies,
  }).annotate(Tool.Readonly, true),
  Tool.make("exos_bind_workstream", {
    description:
      "Bind this thread to an existing workstream in its configured Exos project with an explicit role, register the real T3 thread ID with Exos, and enable automatic private transcript capture. Use only for the user's chosen workstream. Assignment and role are immutable. Check registered and error fields; a local binding alone does not mean the hub accepted it. Calling again with the same assignment resumes a detached binding.",
    parameters: Schema.Struct({ workstream: ExosName, role: ExosRole }),
    success: ExosBinding,
    failure: ExosError,
    dependencies,
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true),
  Tool.make("exos_detach_workstream", {
    description:
      "Stop this thread's automatic Exos registration and capture. Keeps captured files and hub session history. Does not close the workstream, complete tasks, or merge code.",
    success: ExosBinding,
    failure: ExosError,
    dependencies,
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true),
  Tool.make("exos_sync", {
    description:
      "Retry pending Exos registration and refresh this thread's private transcript capture. Check registered, registrationError and captureError. Archive capturePath with source other and the actual thread ID. Exos native telemetry ingestion does not consume this export.",
    success: Schema.NullOr(ExosBinding),
    failure: ExosError,
    dependencies,
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true),
  Tool.make("exos_list_skills", {
    description:
      "Discover current Exos skills from the project's configured skillsDirectory on the T3 server. Read the relevant skill before performing an Exos workflow; never infer unavailable skill instructions.",
    success: Schema.Array(Schema.String),
    failure: ExosError,
    dependencies,
  }).annotate(Tool.Readonly, true),
  Tool.make("exos_read_skill", {
    description:
      "Read an installed Exos skill or a referenced file within that skill's directory. T3 session binding and capture use exos_bind_workstream and exos_sync. Clave-only groups, messages and views require an available T3 equivalent; do not claim unavailable Clave actions ran. Preserve independent verification and human merge/workstream-close gates. Project context loads only on request.",
    parameters: Schema.Struct({
      name: ExosName,
      file: Schema.optional(Schema.String.check(Schema.isMaxLength(512))),
    }),
    success: Schema.String,
    failure: ExosError,
    dependencies,
  }).annotate(Tool.Readonly, true),
);
