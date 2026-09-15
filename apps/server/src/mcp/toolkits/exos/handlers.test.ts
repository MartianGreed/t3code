import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { McpSchema, McpServer } from "effect/unstable/ai";
import { ExosIntegration } from "../../../exos/ExosIntegration.ts";
import { ExosToolkitRegistrationLive } from "../../McpHttpServer.ts";
import { McpInvocationContext } from "../../McpInvocationContext.ts";

it.effect.each(["codex", "claudeCode"])(
  "serves native Exos tools to %s using the authenticated thread",
  (provider) => {
    const calls: string[] = [];
    const threadId = ThreadId.make("authenticated-thread");
    return Effect.gen(function* () {
      const server = yield* McpServer.McpServer;
      const invoke = (arguments_: Record<string, string>) =>
        server.callTool({ name: "exos_status", arguments: arguments_ }).pipe(
          Effect.provideService(McpInvocationContext, {
            environmentId: EnvironmentId.make("environment"),
            threadId,
            providerSessionId: "session",
            providerInstanceId: ProviderInstanceId.make(provider),
            capabilities: new Set<import("../../McpInvocationContext.ts").McpCapability>(),
            issuedAt: 1,
          }),
          Effect.provideService(McpSchema.McpServerClient, {
            clientId: 1,
            clientCapabilities: {},
            clientInfo: { name: provider, version: "test" },
            protocolVersion: "2025-06-18",
            initializePayload: {
              protocolVersion: "2025-06-18",
              capabilities: {},
              clientInfo: { name: provider, version: "test" },
            },
            getClient: Effect.die("unused"),
          }),
        );
      const result = yield* invoke({});
      expect(result.isError).not.toBe(true);
      expect(calls).toEqual([threadId]);
      const invalid = yield* Effect.exit(invoke({ threadId: "another-thread" }));
      expect(invalid._tag).toBe("Failure");
      expect(calls).toEqual([threadId]);
    }).pipe(
      Effect.provide(
        ExosToolkitRegistrationLive.pipe(
          Layer.provideMerge(McpServer.McpServer.layer),
          Layer.provide(
            Layer.mock(ExosIntegration)({
              status: (id) =>
                Effect.sync(() => {
                  calls.push(id);
                  return { enabled: true, threadId: id, project: "demo", binding: null };
                }),
            }),
          ),
        ),
      ),
    );
  },
);
