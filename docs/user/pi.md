# Pi

Install [Pi](https://github.com/earendil-works/pi) 0.87.1 or newer, then run `pi` and use `/login` to connect your account. Pi also supports provider API keys and compatible local servers configured in its `models.json`.

In T3 Code, open Settings → Providers and enable Pi. Set its binary path if `pi` is not on your server's PATH. Models are discovered from the Pi profile on the machine running the T3 server. Refresh the provider after changing credentials, models, or extensions.

Create a thread, select a Pi model, and choose Full access. Pi's tools run with your account's filesystem and command access. T3's approval and sandbox modes are not available for Pi. Pi approval extensions can still ask for confirmation or input through the chat.

Image uploads are sent as images for models that support vision. Other file attachments are supplied as local paths for Pi to inspect. Threads retain Pi's session file so you can resume after restarting T3. Pi's automatic context compaction stays enabled; T3's compact action also invokes Pi's native compaction.

Pi skills, prompt templates, and extension commands appear in the slash-command picker. Terminal-only extension interfaces are subject to [Pi's RPC limitations](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/rpc-extension-ui.md). Plan mode and conversation rollback are unavailable.

For separate Pi profiles, add another Pi provider instance and set its `PI_CODING_AGENT_DIR` environment variable to that profile's directory. Credentials, settings, models, and extensions follow that profile.
