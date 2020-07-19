import click
import commands

@click.group()
@click.version_option()
def cli():
    """CLI for Flink Commands"""


commands.register_commands(cli)