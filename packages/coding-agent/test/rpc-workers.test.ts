import { afterEach, describe, expect, test } from "bun:test";
import * as path from "node:path";
import type { AgentSession } from "@oh-my-pi/pi-coding-agent";
import { bindAgentToolSession } from "@oh-my-pi/pi-coding-agent/session/agent-tool-session";
import { RpcClient } from "@oh-my-pi/pi-coding-agent/modes/rpc/rpc-client";
import { RpcWorkerController, RpcWorkerError } from "@oh-my-pi/pi-coding-agent/modes/rpc/rpc-workers";
import type { RpcWorkerFrame } from "@oh-my-pi/pi-coding-agent/modes/rpc/rpc-types";
import { AgentLifecycleManager } from "@oh-my-pi/pi-coding-agent/registry/agent-lifecycle";
import { AgentRegistry } from "@oh-my-pi/pi-coding-agent/registry/agent-registry";
import type { StructuredSubagentRequest } from "@oh-my-pi/pi-coding-agent/task/structured-subagent";
import type { ToolSession } from "@oh-my-pi/pi-coding-agent/tools";
import { EventBus } from "@oh-my-pi/pi-coding-agent/utils/event-bus";

const controllers: RpcWorkerController[] = [];

afterEach(async () => {
	await Promise.all(controllers.splice(0).map(controller => controller.dispose()));
});

function createHarness(
	options: {
		spawn?: (request: StructuredSubagentRequest) => Promise<void>;
		workerSession?: AgentSession;
		workerIds?: string[];
	} = {},
) {
	const frames: RpcWorkerFrame[] = [];
	const eventBus = new EventBus();
	const registry = new AgentRegistry();
	const lifecycle = new AgentLifecycleManager(registry);
	const rootSession = {
		cwd: process.cwd(),
		hasUI: false,
		settings: {
			get: (key: string) => {
				if (key === "task.enableLsp") return false;
				if (key === "task.maxRuntimeMs") return 0;
				return undefined;
			},
		},
		getAgentId: () => "Main",
		getSessionFile: () => null,
	} as unknown as ToolSession;
	registry.register({
		id: "Main",
		displayName: "Main",
		kind: "main",
		session: rootSession as unknown as AgentSession,
	});
	const requests: StructuredSubagentRequest[] = [];
	let spawnCount = 0;
	const spawn =
		options.spawn ??
		(async request => {
			spawnCount++;
			requests.push(request);
			const workerSession =
				options.workerSession ??
				({
					isStreaming: false,
					prompt: async () => true,
					steer: async () => {},
					followUp: async () => {},
					abort: async () => {},
					dispose: async () => {},
				} as unknown as AgentSession);
			const workerId = request.identity?.id ?? "WorkerA";
			const childToolSession = {
				...rootSession,
				getAgentId: () => workerId,
			} as ToolSession;
			bindAgentToolSession(workerSession, childToolSession);
			registry.register({
				id: workerId,
				displayName: workerId,
				kind: "sub",
				parentId: request.session.getAgentId?.() ?? "Main",
				session: workerSession,
				status: "running",
			});
			eventBus.emit("task:subagent:lifecycle", {
				id: workerId,
				index: 0,
				agent: "task",
				agentSource: "bundled",
				status: "started",
			});
		});
	const controller = new RpcWorkerController({
		rootSession,
		eventBus,
		output: frame => frames.push(frame),
		registry,
		lifecycle,
		reserveId: async () => options.workerIds?.shift() ?? "WorkerA",
		preflight: async () => {},
		spawn,
	});
	controllers.push(controller);
	return { controller, eventBus, frames, registry, requests, getSpawnCount: () => spawnCount };
}

describe("RpcWorkerController", () => {
	test("acknowledges host-created Workers before emitting authoritative creation and lifecycle effects", async () => {
		const { controller, frames, getSpawnCount } = createHarness();
		const result = await controller.createWorker({ id: "cmd-create", type: "create_worker", task: "Inspect auth" });

		expect(result).toEqual({
			commandId: "cmd-create",
			workerId: "WorkerA",
			providerWorkerId: "WorkerA",
			parentWorkerId: "Main",
			status: "acknowledged",
		});
		expect(frames).toEqual([]);

		await controller.drain();
		expect(getSpawnCount()).toBe(1);
		expect(frames.map(frame => frame.type)).toEqual(["worker_created", "worker_lifecycle_changed"]);
		expect(frames[0]).toMatchObject({
			commandId: "cmd-create",
			workerId: "WorkerA",
			providerWorkerId: "WorkerA",
			parentWorkerId: "Main",
			sequence: 1,
		});
	});

	test("deduplicates a repeated create command id without spawning another Worker", async () => {
		const { controller, getSpawnCount } = createHarness();
		const command = { id: "cmd-create", type: "create_worker" as const, task: "Inspect auth" };

		const first = await controller.createWorker(command);
		const repeated = await controller.createWorker(command);
		await controller.drain();

		expect(repeated).toEqual(first);
		expect(getSpawnCount()).toBe(1);
	});

	test("rejects an unknown parent before acknowledgement", async () => {
		const { controller, frames, getSpawnCount } = createHarness();

		await expect(
			controller.createWorker({
				id: "cmd-create",
				type: "create_worker",
				task: "Inspect auth",
				parentWorkerId: "Missing",
			}),
		).rejects.toMatchObject({ code: "unknown_worker" });
		expect(frames).toEqual([]);
		expect(getSpawnCount()).toBe(0);
	});

	test("acknowledges direct Worker steering separately from its observed effect", async () => {
		let steered = "";
		const workerSession = {
			isStreaming: true,
			steer: async (message: string) => {
				steered = message;
			},
			dispose: async () => {},
		} as unknown as AgentSession;
		const { controller, frames } = createHarness({ workerSession });
		await controller.createWorker({ id: "cmd-create", type: "create_worker", task: "Inspect auth" });
		await controller.drain();
		frames.length = 0;

		const result = await controller.commandWorker({
			id: "cmd-steer",
			type: "command_worker",
			workerId: "WorkerA",
			action: "steer",
			message: "Check the migration too",
		});

		expect(result).toEqual({ commandId: "cmd-steer", workerId: "WorkerA", action: "steer", acknowledged: true });
		expect(frames).toEqual([]);
		await controller.drain();
		expect(steered).toBe("Check the migration too");
		expect(frames).toEqual([
			expect.objectContaining({
				type: "worker_command_effect",
				commandId: "cmd-steer",
				workerId: "WorkerA",
				action: "steer",
				outcome: "effectObserved",
			}),
		]);
	});

	test("fails closed when a Worker revision is stale", async () => {
		const { controller } = createHarness();
		await controller.createWorker({ id: "cmd-create", type: "create_worker", task: "Inspect auth" });
		await controller.drain();

		try {
			await controller.commandWorker({
				id: "cmd-steer",
				type: "command_worker",
				workerId: "WorkerA",
				action: "steer",
				message: "Check the migration too",
				expectedWorkerRevision: 0,
			});
			throw new Error("Expected stale revision rejection");
		} catch (error) {
			expect(error).toBeInstanceOf(RpcWorkerError);
			expect(error).toMatchObject({ code: "stale_worker_revision" });
		}
	});

	test("deduplicates direct Worker commands by command id", async () => {
		let steerCount = 0;
		const workerSession = {
			isStreaming: true,
			steer: async () => {
				steerCount++;
			},
			dispose: async () => {},
		} as unknown as AgentSession;
		const { controller, frames } = createHarness({ workerSession });
		await controller.createWorker({ id: "cmd-create", type: "create_worker", task: "Inspect auth" });
		await controller.drain();
		frames.length = 0;
		const command = {
			id: "cmd-steer",
			type: "command_worker" as const,
			workerId: "WorkerA",
			action: "steer" as const,
			message: "Check migration",
		};

		const first = await controller.commandWorker(command);
		const repeated = await controller.commandWorker(command);
		expect(repeated).toEqual(first);
		await controller.drain();
		expect(steerCount).toBe(1);
		expect(frames.filter(frame => frame.type === "worker_command_effect")).toHaveLength(1);
	});

	test("rejects reuse of a command id with different input", async () => {
		const { controller } = createHarness();
		await controller.createWorker({ id: "cmd-create", type: "create_worker", task: "Inspect auth" });
		await expect(
			controller.createWorker({ id: "cmd-create", type: "create_worker", task: "Inspect billing" }),
		).rejects.toMatchObject({ code: "duplicate_command" });
	});

	test("creates nested Workers with their parent tool context", async () => {
		const { controller, registry, requests } = createHarness({ workerIds: ["WorkerA", "WorkerB"] });
		await controller.createWorker({ id: "cmd-parent", type: "create_worker", task: "Inspect auth" });
		await controller.drain();

		const nested = await controller.createWorker({
			id: "cmd-child",
			type: "create_worker",
			task: "Inspect token refresh",
			parentWorkerId: "WorkerA",
		});
		await controller.drain();

		expect(nested).toMatchObject({ workerId: "WorkerB", parentWorkerId: "WorkerA" });
		expect(requests[1]?.session.getAgentId?.()).toBe("WorkerA");
		expect(registry.get("WorkerB")?.parentId).toBe("WorkerA");
	});

	test("rejects a parent Worker owned by another root Session", async () => {
		const { controller, registry } = createHarness();
		const foreignSession = {} as AgentSession;
		registry.register({ id: "Other", displayName: "Other", kind: "main", session: foreignSession });
		registry.register({
			id: "Foreign",
			displayName: "Foreign",
			kind: "sub",
			parentId: "Other",
			session: foreignSession,
		});

		await expect(
			controller.createWorker({
				id: "cmd-foreign",
				type: "create_worker",
				task: "Inspect foreign state",
				parentWorkerId: "Foreign",
			}),
		).rejects.toMatchObject({ code: "unknown_worker" });
	});

	test("publishes transcript byte cursors with monotonic Worker revisions", async () => {
		const { controller, frames } = createHarness();
		await controller.createWorker({ id: "cmd-create", type: "create_worker", task: "Inspect auth" });
		await controller.drain();
		frames.length = 0;

		const firstRevision = controller.noteTranscriptRead("WorkerA", 512);
		const secondRevision = controller.noteTranscriptRead("WorkerA", 1024);

		expect(secondRevision).toBe(firstRevision + 1);
		expect(frames).toEqual([
			expect.objectContaining({
				type: "worker_transcript_updated",
				workerId: "WorkerA",
				workerRevision: firstRevision,
				nextByte: 512,
			}),
			expect.objectContaining({
				type: "worker_transcript_updated",
				workerId: "WorkerA",
				workerRevision: secondRevision,
				nextByte: 1024,
			}),
		]);
	});

	test("marks failed Worker lifecycle events terminal", async () => {
		const { controller, eventBus, frames } = createHarness();
		await controller.createWorker({ id: "cmd-create", type: "create_worker", task: "Inspect auth" });
		await controller.drain();
		frames.length = 0;

		eventBus.emit("task:subagent:lifecycle", {
			id: "WorkerA",
			index: 0,
			agent: "task",
			agentSource: "bundled",
			status: "failed",
		});

		expect(frames).toEqual([
			expect.objectContaining({ type: "worker_lifecycle_changed", status: "failed", isTerminal: true }),
		]);
	});
});

describe("RpcClient Worker surface", () => {
	test("preserves host command IDs and routes authoritative Worker frames", async () => {
		using client = new RpcClient({
			cliPath: path.join(process.cwd(), "test", "fixtures", "mock-rpc-agent.ts"),
		});
		const frames: RpcWorkerFrame[] = [];
		client.onWorkerEvent(frame => frames.push(frame));
		await client.start();

		const created = await client.createWorker({ id: "maestro-create-1", task: "Inspect auth" });
		expect(created).toEqual({
			commandId: "maestro-create-1",
			workerId: "WorkerA",
			providerWorkerId: "WorkerA",
			parentWorkerId: "Main",
			status: "acknowledged",
		});
		expect(frames).toContainEqual(
			expect.objectContaining({ type: "worker_created", commandId: "maestro-create-1", workerId: "WorkerA" }),
		);

		const commanded = await client.commandWorker({
			id: "maestro-steer-1",
			workerId: "WorkerA",
			action: "steer",
			message: "Check the migration path",
		});
		expect(commanded).toEqual({
			commandId: "maestro-steer-1",
			workerId: "WorkerA",
			action: "steer",
			acknowledged: true,
		});

		// A following response is an ordering barrier for the prior effect frame.
		await client.getState();
		expect(frames).toContainEqual(
			expect.objectContaining({
				type: "worker_command_effect",
				commandId: "maestro-steer-1",
				workerId: "WorkerA",
				outcome: "effectObserved",
			}),
		);
	}, 20_000);
});
