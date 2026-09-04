import type { AgentSession } from "./agent-session";
import type { ToolSession } from "../tools";

const toolSessions = new WeakMap<AgentSession, ToolSession>();

/** Bind the execution context used to construct tools for an AgentSession. */
export function bindAgentToolSession(session: AgentSession, toolSession: ToolSession): void {
	toolSessions.set(session, toolSession);
}

/** Return the live tool execution context for an AgentSession, when SDK-created. */
export function getAgentToolSession(session: AgentSession): ToolSession | undefined {
	return toolSessions.get(session);
}
