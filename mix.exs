defmodule Kafun.MixProject do
  use Mix.Project

  def project do
    [
      app: :kafun,
      version: "0.4.2",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      releases: releases()
    ]
  end

  defp releases do
    [
      kafun: [
        # Bundle ERTS so the target box doesn't need Erlang/Elixir installed.
        include_executables_for: [:unix],
        include_erts: true,
        applications: [
          runtime_tools: :permanent
        ],
        # Strip beams; significant size win.
        strip_beams: true
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {Kafun.Application, []}
    ]
  end

  defp deps do
    [
      {:bandit, "~> 1.6"},
      {:plug, "~> 1.16"},
      {:exqlite, "~> 0.27"},
      {:saxy, "~> 1.5"},
      {:telemetry, "~> 1.3"},
      # Admin UI.
      {:phoenix, "~> 1.7"},
      {:phoenix_live_view, "~> 1.0"},
      {:phoenix_html, "~> 4.1"},
      {:phoenix_pubsub, "~> 2.1"},
      {:jason, "~> 1.4"},
      # Migration / outbound HTTP.
      {:req, "~> 0.5"},
      # LiveView test DOM parsing.
      {:lazy_html, ">= 0.1.0", only: :test}
    ]
  end
end
