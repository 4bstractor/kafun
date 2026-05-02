defmodule Kafun.MixProject do
  use Mix.Project

  def project do
    [
      app: :kafun,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps()
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
      {:telemetry, "~> 1.3"}
    ]
  end
end
