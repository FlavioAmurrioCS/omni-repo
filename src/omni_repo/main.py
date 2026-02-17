from __future__ import annotations

import asyncio
import logging
import os
import shlex
import subprocess
from collections import Counter
from functools import cached_property
from functools import wraps
from typing import TYPE_CHECKING
from typing import Annotated
from typing import Any
from typing import NamedTuple
from typing import NoReturn
from urllib.parse import urlparse

import anyio
import typer

if TYPE_CHECKING:
    from collections.abc import Callable
    from collections.abc import Coroutine

    from typing_extensions import ParamSpec
    from typing_extensions import TypeVar

    P = ParamSpec("P")
    R = TypeVar("R")

logger = logging.getLogger(__name__)


class OmniRepo(NamedTuple):
    repo: str
    folder: str
    tags: set[str]

    async def async_clone(self) -> subprocess.CompletedProcess[bytes]:
        cmd = ("git", "clone", "--single-branch", self.repo, self.folder)
        path = anyio.Path(self.folder)
        if await path.exists():
            print(f"Folder {self.folder} already exists, skipping clone.")
            return subprocess.CompletedProcess(cmd, 0, b"", b"")
        print(f"Cloning {self.repo}...")
        return await async_subprocess_run(cmd, capture_output=True)

    async def execute(  # noqa: C901, PLR0911, PLR0913
        self,
        *,
        cmd: tuple[str, ...],
        parallel: bool,
        if_file: list[str] | None,
        if_not_file: list[str] | None,
        if_shell: str | None,
        if_dirty: bool | None,
        # check_cmd: bool | None,
        banner: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        ret = subprocess.CompletedProcess(cmd, 0, b"", b"")
        if not await anyio.Path(self.folder).exists():
            return ret
        if if_file and not all(
            [(await anyio.Path(os.path.join(self.folder, x)).exists()) for x in if_file]
        ):
            return ret
        if if_not_file and any(
            [(await anyio.Path(os.path.join(self.folder, x)).exists()) for x in if_not_file]
        ):
            return ret
        if if_shell:
            result = await async_subprocess_run(
                ("sh", "-c", if_shell), cwd=self.folder, capture_output=True
            )
            if result.returncode != 0:
                return ret
        if if_dirty is not None:
            result_output = (
                await async_subprocess_run(
                    ("git", "status", "--porcelain"), cwd=self.folder, capture_output=True
                )
            ).stdout.strip()
            if (if_dirty and not result_output) or (not if_dirty and result_output):
                return ret
        banner_str = (
            "################################################################################\n"
            f"# {self.repo}\n"
            "################################################################################\n"
        )
        cmd = tuple(x if x != "." else self.folder for x in cmd)
        if parallel:
            buffer = banner_str if banner else ""

            r = await async_subprocess_run(cmd=cmd, cwd=self.folder, capture_output=True)
            if r.stderr:
                buffer += r.stderr.decode()
            if r.stdout:
                buffer += r.stdout.decode()
            print(buffer, end="")
            return r

        if banner:
            print(banner_str, end="")
        return await async_subprocess_run(cmd=cmd, cwd=self.folder, capture_output=False)


DEFAULT_WORKDIR = "~/omni-repo"
DEFAULT_REPOS_FILE = "~/omni-repo/repos.txt"


def repo_to_dirname(repo: str) -> str:
    if repo.startswith("git@"):
        repo = "https://" + repo[4:].replace(":", "/", 1)
    p = urlparse(repo)
    return os.path.join(p.netloc, p.path.lstrip("/"))


# def to_sync(func: Callable[P, Awaitable[R]]) -> Callable[P, R]:
#     @wraps(func)
#     def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
#         promise = func(*args, **kwargs)
#         return asyncio.run(promise)  # pyright: ignore[reportArgumentType]


#     return wrapper
def to_sync(func: Callable[P, Coroutine[Any, Any, R]]) -> Callable[P, R]:
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        return asyncio.run(func(*args, **kwargs))

    return wrapper


def with_exit_code(func: Callable[P, int]) -> Callable[P, NoReturn]:
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> NoReturn:
        exit_code = 1
        try:
            exit_code = func(*args, **kwargs)
        except Exception:  # noqa: BLE001
            exit_code = 1
        raise SystemExit(exit_code)

    return wrapper


async def async_subprocess_run(
    cmd: tuple[str, ...], *, capture_output: bool = False, cwd: str | None = None
) -> subprocess.CompletedProcess[bytes]:
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE if capture_output else None,
        stderr=asyncio.subprocess.PIPE if capture_output else None,
        cwd=cwd,
    )
    logger.debug("PID: %d, Command: %s", process.pid, " ".join(shlex.quote(x) for x in cmd))
    stdout_bytes, stderr_bytes = await process.communicate()
    exit_code = await process.wait()
    logger.debug("PID: %d, Exit code: %d", process.pid, exit_code)
    return subprocess.CompletedProcess(cmd, exit_code, stdout_bytes, stderr_bytes)


class OmniRepoCLI:
    def __init__(
        self,
        *,
        workdir: str = typer.Option(
            DEFAULT_WORKDIR,
            help="Base directory where repositories will be cloned",
            envvar="OMNI_REPO_WORKDIR",
        ),
        repos_file: str = typer.Option(
            DEFAULT_REPOS_FILE,
            help="Path to the file containing the list of repositories",
            envvar="OMNI_REPO_REPOS_FILE",
        ),
        parallel: Annotated[
            bool,
            typer.Option(
                "--parallel",
                "-p",
                help="Run tasks in parallel",
                show_default=True,
                envvar="OMNI_REPO_PARALLEL",
            ),
        ] = False,
    ) -> None:
        """"""
        self.workdir = workdir
        self.repos_file = repos_file
        self.parallel = parallel

    @cached_property
    def repos(self) -> tuple[OmniRepo, ...]:
        workdir = os.path.expanduser(self.workdir)
        with open(os.path.expanduser(self.repos_file)) as f:
            return tuple(
                OmniRepo(
                    folder=os.path.join(workdir, repo_to_dirname(line.strip())),
                    repo=line.strip(),
                    tags=set(),
                )
                for line in f
                if line.strip() and not line.strip().startswith("#")
            )

    async def clone(self) -> int:
        """
        Clone all repositories.
        """
        promises = [omni_repo.async_clone() for omni_repo in self.repos]
        results = (
            await asyncio.gather(*promises)
            if self.parallel
            else [await promise for promise in promises]
        )
        for result in results:
            if result.returncode != 0:
                print(
                    ("#" * 100)
                    + "\n"
                    + (result.stderr or result.stdout).decode()
                    + "\n"
                    + ("#" * 100)
                )
        counter = Counter(result.returncode for result in results)
        counter.pop(0, None)  # Remove successful executions
        if counter:
            return counter.most_common(1)[0][0]  # Return the most common non-zero exit code
        return 0

    def vscode_ws(self) -> None: ...
    def create_pr(self) -> None: ...
    async def run(  # noqa: PLR0913
        self,
        *,
        ctx: typer.Context,
        if_file: Annotated[
            list[str] | None,
            typer.Option("--if-file", help="Only run if this file exists in the repo"),
        ] = None,
        if_not_file: Annotated[
            list[str] | None,
            typer.Option("--if-not-file", help="Only run if this file does not exist in the repo"),
        ] = None,
        if_shell: Annotated[
            str | None, typer.Option(help="Only run if this shell command succeeds")
        ] = None,
        if_dirty: Annotated[
            bool | None, typer.Option(help="Only run if the repository is dirty")
        ] = None,
        # check_cmd: Annotated[
        #     bool | None, typer.Option("--check-cmd", help="Only run if this command succeeds")
        # ] = False,
        banner: Annotated[bool, typer.Option("--banner", help="Display a banner")] = False,
    ) -> int:
        """
        Run a command in all repositories.
        """
        cmd = tuple(ctx.args)
        promises = [
            omni_repo.execute(
                if_file=if_file,
                if_not_file=if_not_file,
                if_shell=if_shell,
                if_dirty=if_dirty,
                # check_cmd=check_cmd,
                banner=banner,
                cmd=cmd,
                parallel=self.parallel,
            )
            for omni_repo in self.repos
        ]

        results = (
            await asyncio.gather(*promises)
            if self.parallel
            else [await promise for promise in promises]
        )

        counter = Counter(result.returncode for result in results)
        counter.pop(0, None)  # Remove successful executions
        if counter:
            return counter.most_common(1)[0][0]  # Return the most common non-zero exit code
        return 0

    def get_app(self) -> typer.Typer:
        app = typer.Typer()
        app.callback()(self.__init__)  # type: ignore[misc]
        app.command()(with_exit_code(to_sync(self.clone)))
        # app.command()(self.vscode_ws)
        # app.command()(self.create_pr)
        app.command(
            # add_help_option=False,
            # no_args_is_help=False,
            context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
        )(with_exit_code(to_sync(self.run)))

        return app


cli = OmniRepoCLI()
app = cli.get_app()

if __name__ == "__main__":
    app()
