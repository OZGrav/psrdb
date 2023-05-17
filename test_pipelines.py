import pytest
import json
from django.contrib.auth.models import Permission
from django.contrib.contenttypes.models import ContentType
from datetime import datetime

from dataportal.models import Pipelines

from cli.tables.pipelines import Pipelines as CliPipelines
from cli.graphql_client import GraphQLClient
from cli.utils import obtain_psrdb_subparser, obtain_default_args, obtain_jwt_token, jwt_mutation, jwt_vars_creator


def __create_creator(client, django_user_model):
    username = "creator"
    password = "rotaerc"
    creator = django_user_model.objects.create_user(username=username, password=password)
    content_type = ContentType.objects.get_for_model(Pipelines)
    permission = Permission.objects.get(content_type=content_type, codename="add_pipelines")
    creator.user_permissions.add(permission)
    return creator


# Test cli.tables.pipelines
def test_cli_pipeline_parser():
    subparser = obtain_psrdb_subparser()
    pipeline_parser = subparser.add_parser(CliPipelines.get_name(), help=CliPipelines.get_description())
    CliPipelines.configure_parsers(pipeline_parser)


@pytest.mark.django_db
def test_cli_pipeline_create(client, django_user_model):
    user = __create_creator(client, django_user_model)
    assert user.has_perm("dataportal.add_pipelines")

    jwt_token = obtain_jwt_token(client, jwt_mutation, jwt_vars_creator)
    args = obtain_default_args()
    args.subcommand = "create"
    args.name = "name"
    args.description = "description"
    args.revision = "revision"
    args.created_at = "1970-01-01T00:00:00Z"
    args.created_by = "created_by"
    args.configuration = "{configuration: none}"

    t = CliPipelines(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200
    assert response.content == b'{"data":{"createPipeline":{"pipeline":{"id":"1"}}}}'


@pytest.mark.django_db
def test_cli_pipeline_list(client, django_user_model):
    user = __create_creator(client, django_user_model)
    # assert user.has_perm("dataportal.add_pipelines")

    jwt_token = obtain_jwt_token(client, jwt_mutation, jwt_vars_creator)
    args = obtain_default_args()
    args.subcommand = "list"
    args.id = None
    args.name = None

    t = CliPipelines(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200
    assert response.content == b'{"data":{"pipelines":[]}}'


@pytest.mark.django_db
def test_cli_pipeline_list_with_name(client, django_user_model):
    user = __create_creator(client, django_user_model)

    jwt_token = obtain_jwt_token(client, jwt_mutation, jwt_vars_creator)
    args = obtain_default_args()
    args.subcommand = "list"
    args.id = None
    args.name = "name"

    t = CliPipelines(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200
    assert response.content == b'{"data":{"pipelinesByName":[]}}'


@pytest.mark.django_db
def test_cli_pipeline_list_with_id(client, django_user_model):
    user = __create_creator(client, django_user_model)

    jwt_token = obtain_jwt_token(client, jwt_mutation, jwt_vars_creator)
    args = obtain_default_args()
    args.subcommand = "list"
    args.id = 2
    args.name = None

    t = CliPipelines(client, "/graphql/", jwt_token)
    response = t.process(args)
    print(json.loads(response.content))
    assert response.status_code == 200
    # assert response.content == b'{"data":{"pipelineById":[]}}'


@pytest.mark.django_db
def test_cli_pipeline_update(client, django_user_model):
    user = __create_creator(client, django_user_model)
    assert user.has_perm("dataportal.add_pipelines")

    jwt_token = obtain_jwt_token(client, jwt_mutation, jwt_vars_creator)
    args = obtain_default_args()

    # first create a record
    args.subcommand = "create"
    args.name = "name"
    args.description = "description"
    args.revision = "revision"
    args.created_at = "1970-01-01T00:00:00Z"
    args.created_by = "created_by"
    args.configuration = "{configuration: none}"

    t = CliPipelines(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200
    assert response.content == b'{"data":{"createPipeline":{"pipeline":{"id":"2"}}}}'

    content = json.loads(response.content)
    # then udpate a record
    args.subcommand = "update"
    args.id = int(content["data"]["createPipeline"]["pipeline"]["id"])
    args.name = "name2"
    args.description = "description2"
    args.revision = "revision2"
    args.created_at = "1970-01-01T00:00:01Z"
    args.created_by = "created_by2"
    args.configuration = "{configuration: none2}"

    t = CliPipelines(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200
    assert (
        response.content
        == b'{"data":{"updatePipeline":{"pipeline":{"id":"2","name":"name2","description":"description2","revision":"revision2","createdAt":"1970-01-01T00:00:01+00:00","createdBy":"created_by2","configuration":"{configuration: none2}"}}}}'
    )
