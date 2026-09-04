import type { ThinkingLevel } from "@oh-my-pi/pi-agent-core";
import { setTimeout as scheduleImmediate } from "node:timers";
import type { AgentSession } from "../../session/agent-session";
import { getAgentToolSession } from "../../session/agent-tool-session";
import { type AgentRef, AgentRegistry, MAIN_AGENT_ID, type RegistryEvent } from "../../registry/agent-registry";
import { AgentLifecycleManager } from "../../registry/agent-lifecycle";
import { type SubagentLifecyclePayload, TASK_SUBAGENT_LIFECYCLE_CHANNEL } from "../../task";
import { createEvalCustomTools, describeEvalTools } from "../../task/eval-tools";
import {
	reserveStructuredSubagentId,
	resolveEffectiveSubagentPolicy,
	runStructuredSubagent,
	type StructuredSubagentRequest,
} from "../../task/structured-subagent";
import type { TaskEffort } from "../../thinking";
import type { ToolSession } from "../../tools";
import { isIrcEnabled } from "../../tools/hub";
import type { EventBus } from "../../utils/event-bus";
import type {
	RpcCommandWorkerAction,
	RpcCommandWorkerData,
	RpcCreateWorkerData,
	RpcWorkerCommand,
	RpcWorkerCommandEffectFrame,
	RpcWorkerFrame,
} from "./rpc-types";

const WORKER_ERROR_CODES = [
	"duplicate_command",
	"invalid_request",
	"unknown_worker",
	"stale_worker_revision",
	"unsupported_action",
	"worker_not_mutable",
	"session_busy",
] as const;

export type RpcWorkerErrorCode = (typeof WORKER_ERROR_CODES)[number];

export class RpcWorkerError extends Error {
	constructor(
		readonly code: RpcWorkerErrorCode,
		message: string,
	) {
		super(message);
		this.name = "RpcWorkerError";
	}
}

type CreateWorkerCommand = Extract<RpcWorkerCommand, { type: "create_worker" }>;
type CommandWorkerCommand = Extract<RpcWorkerCommand, { type: "command_worker" }>;

type WorkerSpawn = (request: StructuredSubagentRequest) => Promise<unknown>;
type WorkerPreflight = (request: StructuredSubagentRequest) => Promise<unknown>;
type WorkerIdReservation = (session: ToolSession, label?: string) => Promise<string>;

export interface RpcWorkerControllerOptions {
	rootSession: ToolSession;
	eventBus: EventBus;
	output: (frame: RpcWorkerFrame) => void;
	registry?: AgentRegistry;
	lifecycle?: AgentLifecycleManager;
	reserveId?: WorkerIdReservation;
	preflight?: WorkerPreflight;
	spawn?: WorkerSpawn;
}

type CommandReceipt = {
	signature: string;
	response: RpcCreateWorkerData | RpcCommandWorkerData;
};

type PendingCreation = {
	commandId: string;
	parentWorkerId: string;
};

function commandSignature(command: RpcWorkerCommand): string {
	return JSON.stringify(command);
}

function requireCommandId(id: string | undefined, command: RpcWorkerCommand["type"]): string {
	const value = id?.trim();
	if (!value) throw new RpcWorkerError("invalid_request", `${command} requires a non-empty id`);
	return value;
}

function requireMessage(action: RpcCommandWorkerAction, message: string | undefined): string {
	const value = message?.trim();
	if (!value) throw new RpcWorkerError("invalid_request", `${action} requires a non-empty message`);
	return value;
}

function effortFromThinkingLevel(level: ThinkingLevel | undefined): TaskEffort | undefined {
	if (level === undefined) return undefined;
	if (level === "minimal" || level === "low") return "lo";
	if (level === "medium") return "med";
	return "hi";
}

function isTerminalLifecycle(status: SubagentLifecyclePayload["status"]): boolean {
	return status === "failed" || status === "aborted";
}

function lifecycleStatus(status: SubagentLifecyclePayload["status"]): string {
	if (status === "started") return "running";
	if (status === "completed") return "idle";
	return status;
}

function errorOutcome(action: RpcCommandWorkerAction, error: unknown): RpcWorkerCommandEffectFrame["outcome"] {
	if (action === "cancel") return "cancelled";
	if (action === "interrupt" || action === "kill") return "stopped";
	if (error instanceof RpcWorkerError && error.code === "worker_not_mutable") return "rejected";
	return "failed";
}

/**
 * Owns the host-addressable Worker command ledger and semantic event ordering
 * for one RPC root Session. OMP's AgentRegistry and child AgentSessions remain
 * authoritative; this class exposes them without routing commands through the
 * root prompt or inferring effects from display text.
 */
export class RpcWorkerController {
	readonly #rootSession: ToolSession;
	readonly #rootWorkerId: string;
	readonly #eventBus: EventBus;
	readonly #output: (frame: RpcWorkerFrame) => void;
	readonly #registry: AgentRegistry;
	readonly #lifecycle: AgentLifecycleManager;
	readonly #reserveId: WorkerIdReservation;
	readonly #preflight: WorkerPreflight;
	readonly #spawn: WorkerSpawn;
	readonly #receipts = new Map<string, CommandReceipt>();
	readonly #pendingCreations = new Map<string, PendingCreation>();
	readonly #managedWorkerIds = new Set<string>();
	readonly #workerRevisions = new Map<string, number>();
	readonly #backgroundTasks = new Set<Promise<void>>();
	readonly #artifactCleanups = new Set<() => Promise<void>>();
	readonly #unsubscribeLifecycle: () => void;
	readonly #unsubscribeRegistry: () => void;
	#sequence = 0;
	#disposed = false;

	constructor(options: RpcWorkerControllerOptions) {
		this.#rootSession = options.rootSession;
		this.#rootWorkerId = options.rootSession.getAgentId?.() ?? MAIN_AGENT_ID;
		this.#eventBus = options.eventBus;
		this.#output = options.output;
		this.#registry = options.registry ?? AgentRegistry.global();
		this.#lifecycle = options.lifecycle ?? AgentLifecycleManager.global();
		this.#reserveId = options.reserveId ?? ((session, label) => reserveStructuredSubagentId(session, { label }));
		this.#preflight = options.preflight ?? resolveEffectiveSubagentPolicy;
		this.#spawn = options.spawn ?? runStructuredSubagent;
		this.#unsubscribeLifecycle = this.#eventBus.on(TASK_SUBAGENT_LIFECYCLE_CHANNEL, data => {
			this.#handleLifecycle(data as SubagentLifecyclePayload);
		});
		this.#unsubscribeRegistry = this.#registry.onChange(event => this.#handleRegistryEvent(event));
	}

	getWorkerRevision(workerId: string): number {
		return this.#workerRevisions.get(workerId) ?? 0;
	}

	noteTranscriptRead(workerId: string, nextByte: number): number {
		const current = this.getWorkerRevision(workerId);
		const revision = current + 1;
		this.#workerRevisions.set(workerId, revision);
		this.#output({
			type: "worker_transcript_updated",
			workerId,
			workerRevision: revision,
			nextByte,
			sequence: this.#nextSequence(),
		});
		return revision;
	}

	async createWorker(command: CreateWorkerCommand): Promise<RpcCreateWorkerData> {
		this.#assertOpen();
		const commandId = requireCommandId(command.id, command.type);
		const repeated = this.#getRepeatedReceipt(commandId, command);
		if (repeated) return repeated as RpcCreateWorkerData;
		const task = command.task.trim();
		if (!task) throw new RpcWorkerError("invalid_request", "create_worker requires a non-empty task");

		const { parentSession, parentWorkerId } = await this.#resolveParent(command.parentWorkerId);
		const workerId = await this.#reserveId(parentSession, command.name);
		const customTools = command.tools?.length
			? createEvalCustomTools(parentSession, await describeEvalTools(parentSession, command.tools))
			: undefined;
		const request: StructuredSubagentRequest = {
			session: parentSession,
			invocationKind: "task",
			assignment: task,
			agent: command.agent,
			model: command.model,
			effort: effortFromThinkingLevel(command.thinkingLevel),
			identity: { id: workerId, label: command.name },
			parentToolCallId: commandId,
			detached: true,
			retainArtifacts: true,
			onArtifactsRetained: cleanup => this.#artifactCleanups.add(cleanup),
			keepAlive: true,
			shareEvalSession: true,
			enableLsp: (parentSession.enableLsp ?? true) && parentSession.settings.get("task.enableLsp"),
			enableIrc: isIrcEnabled(parentSession.settings, parentSession.taskDepth ?? 0),
			maxRuntimeMs: parentSession.settings.get("task.maxRuntimeMs"),
			customTools,
		};
		await this.#preflight(request);

		const response: RpcCreateWorkerData = {
			commandId,
			workerId,
			providerWorkerId: workerId,
			parentWorkerId,
			status: "acknowledged",
		};
		this.#receipts.set(commandId, { signature: commandSignature(command), response });
		this.#pendingCreations.set(workerId, { commandId, parentWorkerId });
		this.#managedWorkerIds.add(workerId);
		this.#schedule(async () => {
			try {
				await this.#spawn(request);
			} catch (error) {
				if (!this.#registry.get(workerId)) {
					this.#pendingCreations.delete(workerId);
					this.#emitLifecycle(workerId, "error", true, error instanceof Error ? error.message : String(error));
				}
			}
		});
		return response;
	}

	async commandWorker(command: CommandWorkerCommand): Promise<RpcCommandWorkerData> {
		this.#assertOpen();
		const commandId = requireCommandId(command.id, command.type);
		const repeated = this.#getRepeatedReceipt(commandId, command);
		if (repeated) return repeated as RpcCommandWorkerData;
		const ref = this.#requireMutableWorker(command.workerId);
		const revision = this.getWorkerRevision(ref.id);
		if (command.expectedWorkerRevision !== undefined && command.expectedWorkerRevision !== revision) {
			throw new RpcWorkerError(
				"stale_worker_revision",
				`Worker ${ref.id} revision is ${revision}, not ${command.expectedWorkerRevision}`,
			);
		}
		if (["message", "steer", "follow_up"].includes(command.action)) requireMessage(command.action, command.message);

		const response: RpcCommandWorkerData = {
			commandId,
			workerId: ref.id,
			action: command.action,
			acknowledged: true,
		};
		this.#receipts.set(commandId, { signature: commandSignature(command), response });
		this.#managedWorkerIds.add(ref.id);
		this.#schedule(async () => {
			let outcome: RpcWorkerCommandEffectFrame["outcome"] = "effectObserved";
			let detail: string | undefined;
			try {
				await this.#applyCommand(ref, command);
				if (command.action === "cancel") outcome = "cancelled";
				if (command.action === "interrupt" || command.action === "kill") outcome = "stopped";
			} catch (error) {
				outcome = errorOutcome(command.action, error);
				detail = error instanceof Error ? error.message : String(error);
			}
			this.#workerRevisions.set(ref.id, this.getWorkerRevision(ref.id) + 1);
			this.#output({
				type: "worker_command_effect",
				commandId,
				workerId: ref.id,
				action: command.action,
				outcome,
				sequence: this.#nextSequence(),
				...(detail ? { detail } : {}),
			});
		});
		return response;
	}

	async drain(): Promise<void> {
		while (this.#backgroundTasks.size > 0) {
			await Promise.allSettled(this.#backgroundTasks);
		}
	}

	async dispose(): Promise<void> {
		if (this.#disposed) return;
		this.#disposed = true;
		this.#unsubscribeLifecycle();
		this.#unsubscribeRegistry();
		const releases: Promise<boolean>[] = [];
		for (const workerId of this.#managedWorkerIds) {
			const ref = this.#registry.get(workerId);
			if (ref) releases.push(this.#lifecycle.release(workerId, ref, { tombstone: true }));
		}
		await Promise.allSettled(releases);
		await this.drain();
		await Promise.allSettled([...this.#artifactCleanups].map(cleanup => cleanup()));
		this.#artifactCleanups.clear();
		this.#managedWorkerIds.clear();
	}

	#assertOpen(): void {
		if (this.#disposed) throw new RpcWorkerError("worker_not_mutable", "Worker controller is disposed");
	}

	#getRepeatedReceipt(
		commandId: string,
		command: RpcWorkerCommand,
	): RpcCreateWorkerData | RpcCommandWorkerData | undefined {
		const receipt = this.#receipts.get(commandId);
		if (!receipt) return undefined;
		if (receipt.signature !== commandSignature(command)) {
			throw new RpcWorkerError("duplicate_command", `Command id ${commandId} was already used with different input`);
		}
		return receipt.response;
	}

	async #resolveParent(
		requestedParent: string | undefined,
	): Promise<{ parentSession: ToolSession; parentWorkerId: string }> {
		const parentWorkerId = requestedParent?.trim() || this.#rootWorkerId;
		if (parentWorkerId === this.#rootWorkerId) return { parentSession: this.#rootSession, parentWorkerId };
		const ref = this.#registry.get(parentWorkerId);
		if (!ref || ref.kind !== "sub" || ref.status === "aborted" || !this.#belongsToRoot(ref)) {
			throw new RpcWorkerError("unknown_worker", `Unknown parent Worker: ${parentWorkerId}`);
		}
		try {
			const session = await this.#lifecycle.ensureLive(parentWorkerId);
			const parentSession = getAgentToolSession(session);
			if (!parentSession) {
				throw new RpcWorkerError("worker_not_mutable", `Worker ${parentWorkerId} has no live tool context`);
			}
			return { parentSession, parentWorkerId };
		} catch (error) {
			throw new RpcWorkerError(
				"worker_not_mutable",
				error instanceof Error ? error.message : `Parent Worker ${parentWorkerId} is unavailable`,
			);
		}
	}

	#requireMutableWorker(workerId: string): AgentRef {
		const ref = this.#registry.get(workerId);
		if (!ref || ref.kind !== "sub" || !this.#belongsToRoot(ref)) {
			throw new RpcWorkerError("unknown_worker", `Unknown Worker: ${workerId}`);
		}
		if (ref.status === "aborted") throw new RpcWorkerError("worker_not_mutable", `Worker ${workerId} is terminal`);
		return ref;
	}

	#belongsToRoot(ref: AgentRef): boolean {
		let current: AgentRef | undefined = ref;
		const visited = new Set<string>();
		while (current && !visited.has(current.id)) {
			if (current.parentId === this.#rootWorkerId) return true;
			visited.add(current.id);
			current = current.parentId ? this.#registry.get(current.parentId) : undefined;
		}
		return false;
	}

	async #applyCommand(ref: AgentRef, command: CommandWorkerCommand): Promise<void> {
		if (command.action === "kill") {
			const released = await this.#lifecycle.release(ref.id, ref, { tombstone: true });
			if (!released) throw new RpcWorkerError("worker_not_mutable", `Worker ${ref.id} changed before kill`);
			return;
		}

		let session: AgentSession;
		try {
			session = await this.#lifecycle.ensureLive(ref.id);
		} catch (error) {
			throw new RpcWorkerError(
				"worker_not_mutable",
				error instanceof Error ? error.message : `Worker ${ref.id} is unavailable`,
			);
		}
		switch (command.action) {
			case "message":
				await session.prompt(requireMessage(command.action, command.message), {
					images: command.images,
					...(session.isStreaming ? { streamingBehavior: "steer" as const } : {}),
				});
				return;
			case "steer":
				if (!session.isStreaming) throw new RpcWorkerError("session_busy", `Worker ${ref.id} is not running`);
				await session.steer(requireMessage(command.action, command.message), command.images);
				return;
			case "follow_up":
				if (!session.isStreaming) throw new RpcWorkerError("session_busy", `Worker ${ref.id} is not running`);
				await session.followUp(requireMessage(command.action, command.message), command.images);
				return;
			case "resume":
				if (session.isStreaming) throw new RpcWorkerError("session_busy", `Worker ${ref.id} is already running`);
				await session.prompt(command.message?.trim() || "Continue.", { images: command.images });
				return;
			case "interrupt":
			case "cancel":
				if (!session.isStreaming) throw new RpcWorkerError("worker_not_mutable", `Worker ${ref.id} is not running`);
				await session.abort({
					reason: command.action === "cancel" ? "Cancelled by RPC host" : "Interrupted by RPC host",
				});
				return;
			default:
				throw new RpcWorkerError("unsupported_action", `Unsupported Worker action: ${String(command.action)}`);
		}
	}

	#handleLifecycle(payload: SubagentLifecyclePayload): void {
		if (!this.#managedWorkerIds.has(payload.id)) return;
		const pending = this.#pendingCreations.get(payload.id);
		if (payload.status === "started" && pending) {
			this.#pendingCreations.delete(payload.id);
			this.#output({
				type: "worker_created",
				commandId: pending.commandId,
				workerId: payload.id,
				providerWorkerId: payload.id,
				parentWorkerId: pending.parentWorkerId,
				sequence: this.#nextSequence(),
			});
		}
		this.#emitLifecycle(payload.id, lifecycleStatus(payload.status), isTerminalLifecycle(payload.status));
	}

	#handleRegistryEvent(event: RegistryEvent): void {
		if (event.type !== "status_changed" || event.ref.status !== "aborted") return;
		if (!this.#managedWorkerIds.has(event.ref.id)) return;
		this.#emitLifecycle(event.ref.id, "aborted", true);
	}

	#emitLifecycle(workerId: string, status: string, isTerminal: boolean, detail?: string): void {
		this.#workerRevisions.set(workerId, this.getWorkerRevision(workerId) + 1);
		this.#output({
			type: "worker_lifecycle_changed",
			workerId,
			status,
			sequence: this.#nextSequence(),
			isTerminal,
			...(detail ? { detail } : {}),
		});
	}

	#schedule(operation: () => Promise<void>): void {
		const task = new Promise<void>(resolve => {
			scheduleImmediate(() => {
				void operation().finally(resolve);
			});
		});
		this.#backgroundTasks.add(task);
		void task.finally(() => this.#backgroundTasks.delete(task));
	}

	#nextSequence(): number {
		this.#sequence++;
		return this.#sequence;
	}
}
