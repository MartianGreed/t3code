# Settings and project overrides

The Settings breadcrumb ends with the environment and project a change applies to. They start
at **All environments** and **All projects** and stay selected as you move between categories or
search for a setting.

Preferences saved on this device, such as appearance, confirmations and browser profiles, always
show and ignore the selection. Everything else is stored on a server. Choose one environment to
edit its settings, or leave **All environments** to edit every connected environment at once.
Offline environments keep their current values; this is a bulk edit, not a synced global default.

Choose a project to override settings for it on the selected environments. A layers icon beside
each server row's title shows where the value comes from: the built-in default, the environment,
or a project override. Click it to see that chain on every selected environment. An override can
be reset to inherit again. Settings that cannot be overridden by a project are shown read-only
while a project is selected.

When the selected environments disagree, the control shows **Mixed** in place of a value and the
layers icon turns amber. Picking a value applies it to every selected environment.

Changing an environment value never touches a project's own override. When projects override the
setting you are editing, the layers icon counts them and the chain lists each one with its value:
click a project to jump to it, or **Reset all** to make those projects follow the environment
again.

Providers and diagnostics are per machine: they show one environment at a time, the primary
one until you pick another. Every other setting fans out to the selection.

## Defaults and inheritance

General contains the model and workspace for new threads. Integrations controls agent browser
access. Source Control contains automatic pull, the default pull request merge method and text
generation. The same rows edit environment defaults or project overrides depending on the
project crumb.

The Project category, shown while a project is selected, holds the project's name, icon, actions,
checkouts and removal. Actions belong to a project: editing them creates the project's own list
on each selected environment, and reset returns to the environment's shared list. A project's
`t3.json` actions can be imported there.

For workspace mode, a project's `t3.json` preference applies when the project has no override.
Browser access changes apply when an agent session next starts.

## Exos workstreams

To connect a project to Exos, install the `exos` CLI and sign in on the machine running
the T3 server. Add a project link to the repository's `t3.json`:

```json
{
  "exos": {
    "project": "demo",
    "repository": "../demo-os",
    "skillsDirectory": "../exos/plugin/skills"
  }
}
```

The repository must be the project's OS clone, with a matching `project` in its
`manifest.yaml`. Paths are relative to the T3 project's root, or absolute paths on
the server. For remote connections, these are server paths. `skillsDirectory` is
optional and points at your installed Exos skills. T3 reads those files when asked,
so updating the installed skills updates what agents can discover.

Start a Claude or Codex session and ask it to run `exos_status`. The tools are supplied
by T3 automatically. Ask the agent to use `exos_list_skills` and `exos_read_skill` to
load a workflow. Project context is loaded only when requested.

Choose an existing workstream and a role, then ask the agent to bind the thread with
`exos_bind_workstream`. T3 registers the thread with Exos and captures its conversation
after completed messages and session changes. Registration failures remain visible
in `exos_status`; fix the CLI login or workstream state and run `exos_sync` to retry.
Pending registrations and captures recover when the T3 server restarts.

Captures stay in private files under T3's `userdata/exos/captures`, outside the OS
repository. Each export is limited to 16 MiB; if a transcript exceeds the limit,
T3 keeps the previous export and reports the failure. Archive a capture with the
Exos archive-session skill using `source: other` and its real T3 thread ID. These
exports do not provide Exos's native Clave or Claude telemetry ingestion.

Use `exos_detach_workstream` to stop capture for a thread, or remove the `exos` entry
to disable the project integration. Binding again to the same workstream and role
resumes capture. Use a new thread for another assignment. Detaching retains captures
and the hub's session history; it never closes workstreams, completes tasks, or
merges code. Clave-specific UI and coordination actions still require a corresponding
tool in the current session.

## Project icons

Select the project and open Project to choose an icon, emoji, or image. The choice applies to
every checkout in the project group and appears on connected clients. Choose **Automatic** to let
T3 Code detect an icon again.

When no image is found, web and desktop show a two-character monogram with colors
derived from the saved project name. For example, `Nebula` becomes `NA`,
`Silver Orchard` becomes `SO`, and `M7 Forge` becomes `M7`.

## Keep the default branch current

In Source Control, enable **Automatically pull** to keep the default-branch checkout up to date
with its configured upstream. Choose an environment to set the default or a project to override it.

T3 Code only pulls when it can fast-forward and the checkout has no changed files, untracked files,
or local commits. It skips checkouts on another branch or without an upstream. If a checkout has
local work, resolve it yourself before automatic pulls can resume.
