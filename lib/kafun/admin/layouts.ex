defmodule Kafun.Admin.Layouts do
  @moduledoc "Root and app layouts for the admin UI."

  use Phoenix.Component

  use Phoenix.VerifiedRoutes,
    endpoint: Kafun.Admin.Endpoint,
    router: Kafun.Admin.Router,
    statics: ~w(assets favicon.ico favicon.png)

  import Phoenix.Controller, only: [get_csrf_token: 0]

  embed_templates "layouts/*"
end
