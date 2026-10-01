#!/usr/bin/env node
import { appendFileSync } from "node:fs";

if (process.argv.includes("--version")) {
  process.stdout.write("0.87.1\n");
  process.exit(0);
}
const write = (record) => process.stdout.write(`${JSON.stringify(record)}\n`);
const response = (command, data) =>
  write({ type: "response", id: command.id, command: command.type, success: true, data });
const model = {
  provider: "test",
  id: "model/with-slash",
  name: "Test model",
  reasoning: true,
  contextWindow: 200000,
};
let prompts = 0;
const finish = () => {
  const text = process.env.PI_FIXTURE_OUTPUT ?? "Hello \u2028 world \u2029 🌎";
  write({ type: "message_start", message: { role: "assistant", content: [] } });
  write({
    type: "message_update",
    assistantMessageEvent: { type: "thinking_delta", contentIndex: 0, delta: "Thinking" },
  });
  write({
    type: "message_update",
    assistantMessageEvent: { type: "text_delta", contentIndex: 1, delta: text },
  });
  write({
    type: "message_end",
    message: {
      role: "assistant",
      content: [{ type: "text", text }],
      stopReason: "stop",
      usage: { input: 10, cacheRead: 20, cacheWrite: 5, output: 4, cost: { total: 0.01 } },
    },
  });
  write({
    type: "tool_execution_start",
    toolCallId: "tool-1",
    toolName: "bash",
    args: { command: "pwd" },
  });
  write({
    type: "tool_execution_end",
    toolCallId: "tool-1",
    toolName: "bash",
    result: { content: [{ type: "text", text: "/workspace" }] },
  });
  write({ type: "agent_end", messages: [], willRetry: false });
  write({ type: "agent_settled" });
};
let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let end;
  while ((end = buffer.indexOf("\n")) >= 0) {
    const command = JSON.parse(buffer.slice(0, end));
    buffer = buffer.slice(end + 1);
    if (process.env.PI_FIXTURE_LOG)
      appendFileSync(process.env.PI_FIXTURE_LOG, `${JSON.stringify(command)}\n`);
    switch (command.type) {
      case "get_state":
        response(command, {
          model,
          sessionFile: "/tmp/pi-fixture-session.jsonl",
          sessionId: "pi-session",
        });
        break;
      case "get_available_models":
        response(command, { models: [model] });
        break;
      case "get_commands":
        response(command, {
          commands: [{ name: "skill:test", source: "skill", description: "Test skill" }],
        });
        break;
      case "get_messages":
        response(command, { messages: [] });
        break;
      case "set_model":
        response(command, model);
        break;
      case "set_thinking_level":
        response(command);
        break;
      case "compact":
        write({
          type: "compaction_end",
          reason: "manual",
          result: { tokensBefore: 1000, estimatedTokensAfter: 100 },
          aborted: false,
        });
        response(command, { summary: "compressed" });
        break;
      case "abort":
        response(command);
        write({ type: "agent_settled" });
        break;
      case "prompt":
        prompts++;
        if (process.env.PI_FIXTURE_SCENARIO === "reject") {
          write({
            type: "response",
            id: command.id,
            command: "prompt",
            success: false,
            error: "rejected prompt",
          });
          break;
        }
        response(command, { disposition: "started" });
        if (process.env.PI_FIXTURE_SCENARIO === "crash") {
          process.exit(1);
        }
        if (process.env.PI_FIXTURE_SCENARIO === "retry" && prompts === 1) {
          write({ type: "agent_end", messages: [], willRetry: true });
          write({
            type: "extension_ui_request",
            id: "notice",
            method: "notify",
            message: "retry pending",
          });
          break;
        }
        if (process.env.PI_FIXTURE_SCENARIO === "dialogs") {
          write({
            type: "extension_ui_request",
            id: "confirm-1",
            method: "confirm",
            title: "Proceed?",
          });
          break;
        }
        finish();
        break;
      case "extension_ui_response":
        if (command.id === "confirm-1")
          write({
            type: "extension_ui_request",
            id: "select-1",
            method: "select",
            title: "Choose",
            options: ["A", "B"],
          });
        else finish();
        break;
      default:
        response(command);
    }
  }
});
