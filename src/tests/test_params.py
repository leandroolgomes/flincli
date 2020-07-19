import boto3
from commands.parameters import commands as params
import yaml
import tempfile
from click.testing import CliRunner
from moto import mock_ssm


@mock_ssm
def test_list_no_arg():
    conn = boto3.client('ssm')
    # Create two parameters and check both are returned
    conn.put_parameter(Name='test', Value='testing123', Type='SecureString')
    conn.put_parameter(Name='test1', Value='testing123', Type='SecureString')

    # Call list without any parameters
    out = params.list_params([])

    names = [param['Name'] for param in out]
    assert len(out) == 2
    assert 'test1' in names
    assert 'test' in names


@mock_ssm
def test_list_starts_with():
    conn = boto3.client('ssm')
    # Create two parameters and check both are returned
    conn.put_parameter(Name='test1', Value='testing123', Type='SecureString')
    conn.put_parameter(Name='anotherValue', Value='testing123', Type='SecureString')

    out = params.list_params(["test"])

    names = [param['Name'] for param in out]
    assert len(out) == 1
    assert 'test1' in names
    assert 'anotherValue' not in names


@mock_ssm
def test_delete_correct():
    conn = boto3.client('ssm')
    # Create a parameter and check if it is deleted.
    conn.put_parameter(Name='test1', Value='testing123', Type='SecureString')

    out, err = params.delete_params(['test1'])

    assert len(err) == 0
    assert len(out) == 1
    assert 'test1' in out
    # Check if parameter is actually deleted
    out = params.list_params(['test1'])
    assert len(out) == 0


@mock_ssm
def test_delete_invalid():
    # Delete an invalid parameter
    out, err = params.delete_params(['InvalidParam'])

    assert len(out) == 0
    assert len(err) == 1
    assert 'InvalidParam' in err


@mock_ssm
def test_get_param_correct():
    conn = boto3.client('ssm')

    # Put 2 parameters
    conn.put_parameter(Name='test1', Value='testing1', Type='SecureString')
    conn.put_parameter(Name='test2', Value='testing2', Type='SecureString')

    # get the parameter

    out, err = params.get_params(['test1'])
    # Check output
    assert len(out) == 1
    assert len(err) == 0

    assert out[0]['Name'] == 'test1'
    assert out[0]['Value'] == 'testing1'


@mock_ssm
def test_get_param_incorrect():
    conn = boto3.client('ssm')

    # Put 2 parameters
    conn.put_parameter(Name='test1', Value='testing1', Type='SecureString')
    conn.put_parameter(Name='test2', Value='testing2', Type='SecureString')

    # get the parameter

    out, err = params.get_params(['test1', 'test3'])
    # Check output
    assert len(out) == 1
    assert len(err) == 1

    assert out[0]['Name'] == 'test1'
    assert out[0]['Value'] == 'testing1'
    # Check error
    assert err[0] == 'test3'


@mock_ssm
def test_put_param():
    # Put 1 parameter
    params.put_param(name="test1", value="value1", description='my var', encrypt=True)
    # get the parameter
    out, err = params.get_params(['test1'])
    # Check parameter is returned
    assert len(out) == 1
    assert len(err) == 0

    assert out[0]['Name'] == 'test1'
    assert out[0]['Value'] == 'value1'


@mock_ssm
def test_cli_list_no_arg():
    conn = boto3.client('ssm')
    # Create two parameters and check both are returned
    conn.put_parameter(Name='test1', Value='testing1', Type='SecureString')
    conn.put_parameter(Name='test2', Value='testing2', Type='SecureString')

    # Call list without any parameters
    runner = CliRunner()
    result = runner.invoke(params.list)

    assert "No parameters found" not in result.output
    assert 'test1' in result.output
    assert 'test2' in result.output


@mock_ssm
def test_cli_list_starts_with():
    conn = boto3.client('ssm')
    # Create two parameters and check both are returned
    conn.put_parameter(Name='test1', Value='testing123', Type='SecureString')
    conn.put_parameter(Name='anotherValue', Value='testing123', Type='SecureString')

    runner = CliRunner()
    result = runner.invoke(params.list, ['--name', 'test'])

    assert 'test1' in result.output
    assert 'anotherValue' not in result.output


@mock_ssm
def test_cli_get_param():
    conn = boto3.client('ssm')

    # Create two parameters and check both are returned
    conn.put_parameter(Name='test1', Value='value1', Type='SecureString')
    conn.put_parameter(Name='test2', Value='value2', Type='SecureString')

    runner = CliRunner()
    result = runner.invoke(params.get, ['--name', 'test1'])

    assert 'Invalid Parameters' not in result.output
    assert 'value1' in result.output
    assert 'test1' in result.output

    assert 'test2' not in result.output
    assert 'value2' not in result.output


@mock_ssm
def test_cli_delete_correct():
    conn = boto3.client('ssm')
    # Create a parameter and check if it is deleted.
    conn.put_parameter(Name='test1', Value='testing123', Type='SecureString')

    runner = CliRunner()
    result = runner.invoke(params.delete, ['--name', 'test1'])
    assert 'Deleted Parameters' in result.output
    assert 'test1' in result.output
    # Check if parameter is actually deleted
    runner = CliRunner()
    result = runner.invoke(params.list, ['--name', 'test1'])
    assert 'No parameters found' in result.output


@mock_ssm
def test_cli_delete_invalid():
    # Delete an invalid parameter
    runner = CliRunner()
    result = runner.invoke(params.delete, ['--name', 'invalid_param'])

    assert 'Invalid Parameters' in result.output
    assert 'invalid_param' in result.output
    assert 'Deleted Parameters' not in result.output


@mock_ssm
def test_cli_put_param():
    # Create a parameter
    runner = CliRunner()
    result = runner.invoke(params.put, ['--name', 'test1', '--value', 'value1', '--encrypt'])

    assert 'Created Parameters' in result.output
    assert 'test' in result.output
