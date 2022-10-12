import click
from .cli.manifest_tables import FileSource


def command_depencies():
    class CommandOptionRequiredClass(click.Command):
        def invoke(self, ctx):
            command = ctx.params["command"]
            if command == "manifest_tables":
                source_type = ctx.params["source_type"]
                if source_type == "file":
                    if "directory" not in ctx.params.keys():
                        raise click.ClickException("directory is required")
                    if "filename_mask" not in ctx.params.keys():
                        raise click.ClickException("directory is required")
                else:
                    raise click.ClickException(
                        f"source_type = {source_type} is not valid"
                    )
            else:
                raise click.ClickException(f"command = {command} is not valid")
            super(CommandOptionRequiredClass, self).invoke(ctx)

    return CommandOptionRequiredClass


@click.command(cls=command_depencies())
@click.option(
    "--command",
    default="manifest_tables",
    help="Command indicates what features to perform",
)
@click.option(
    "--source_type",
    default="file",
    help="manifest tables from a directory of data files",
    required=False,
    type=str,
    is_eager=True,
)
@click.option(
    "--directory",
    default=".",
    help="directory of files on which to create the table manifest",
    required=False,
    type=click.Path(exists=True),
    is_eager=True,
)
@click.option(
    "--filename_mask",
    default="*",
    help="filename mask to filter what files to include",
    required=False,
    type=str,
    is_eager=True,
)

@click.option(
    "--extract_regex",
    default="",
    help="regex to extract name from file name",
    required=False,
    type=str,
    is_eager=True,
)
def main(command: str, source_type: str, directory: str, filename_mask: str, extract_regex: str):

    # click.echo('command: {}'.format(command))
    # click.echo('source_type: {}'.format(source_type))
    # click.echo('directory: {}'.format(directory))
    # click.echo('filename_mask: {}'.format(filename_mask))
    if command == "manifest_tables":
        source = FileSource(directory, filename_mask, extract_regex)
        files = source.tables()
        print(files)


if __name__ in ["yetl.__main__", "__main__"]:
    main()
