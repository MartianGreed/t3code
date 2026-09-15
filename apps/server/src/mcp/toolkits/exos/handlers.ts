import * as Effect from "effect/Effect";
import { ExosIntegration } from "../../../exos/ExosIntegration.ts";
import { McpInvocationContext } from "../../McpInvocationContext.ts";
import { ExosToolkit } from "./tools.ts";

const make = Effect.gen(function* () {
  const exos = yield* ExosIntegration;
  return ExosToolkit.of({
    exos_status: () =>
      Effect.flatMap(McpInvocationContext, ({ threadId }) => exos.status(threadId)),
    exos_bind_workstream: ({ workstream, role }) =>
      Effect.flatMap(McpInvocationContext, ({ threadId }) => exos.bind(threadId, workstream, role)),
    exos_detach_workstream: () =>
      Effect.flatMap(McpInvocationContext, ({ threadId }) => exos.detach(threadId)),
    exos_sync: () => Effect.flatMap(McpInvocationContext, ({ threadId }) => exos.sync(threadId)),
    exos_list_skills: () =>
      Effect.flatMap(McpInvocationContext, ({ threadId }) => exos.skills(threadId)),
    exos_read_skill: ({ name, file }) =>
      Effect.flatMap(McpInvocationContext, ({ threadId }) => exos.readSkill(threadId, name, file)),
  });
});
export const ExosToolkitHandlersLive = ExosToolkit.toLayer(make);
