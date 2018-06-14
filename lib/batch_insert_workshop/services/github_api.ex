defmodule BatchInsertWorkshop.GithubApi do
  import Ecto.Query, warn: false
  alias BatchInsertWorkshop.Repo
  alias BatchInsertWorkshop.Model.{Language, GitRepo, RepoLanguage}
  alias BatchInsertWorkshop.{Error, Graphql, Payloads}

  @batch_size 10

  def repos(global_id \\ "") do
    Payloads.GithubPayload.sample()
  end

  def parse() do
    inserted_langs = repos() |> languages |> Enum.map(&{&1.name, &1.id}) |> Enum.into(%{})
    inserted_repos = repos() |> git_repos |> Enum.map(&{&1.name, &1.id}) |> Enum.into(%{})

    repo_langs =
      repos()["nodes"]
      |> Enum.flat_map(fn repo_object ->
        get_in(repo_object, ["languages", "nodes"])
        |> Enum.map(fn lang_object ->
          %{
            git_repo_id: inserted_repos[repo_object["name"]],
            language_id: inserted_langs[lang_object["name"]],
            inserted_at: DateTime.utc_now(),
            updated_at: DateTime.utc_now()
          }
        end)
      end)

    {_, data} =
      Repo.insert_all(
        RepoLanguage,
        repo_langs,
        on_conflict: :replace_all,
        returning: true,
        conflict_target: [:git_repo_id, :language_id]
      )

    data
  end

  def languages(res) do
    langs =
      res["nodes"]
      |> Enum.flat_map(fn repo_object ->
        get_in(repo_object, ["languages", "nodes"])
      end)
      |> Enum.uniq()
      |> Enum.map(
        &[
          name: &1["name"],
          inserted_at: DateTime.utc_now(),
          updated_at: DateTime.utc_now()
        ]
      )

    {_, data} =
      Repo.insert_all(
        Language,
        langs,
        on_conflict: :replace_all,
        returning: true,
        conflict_target: :name
      )

    data
  end

  def git_repos(res) do
    repo_names =
      res["nodes"]
      |> Enum.map(
        &[
          name: &1["name"],
          url: &1["url"],
          inserted_at: DateTime.utc_now(),
          updated_at: DateTime.utc_now()
        ]
      )

    {_, data} =
      Repo.insert_all(
        GitRepo,
        repo_names,
        on_conflict: :replace_all,
        returning: true,
        conflict_target: :name
      )

    data
  end
end
