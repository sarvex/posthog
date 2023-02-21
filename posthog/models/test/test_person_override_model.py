import contextlib
import datetime as dt
from uuid import uuid4

import pytest
from django.db import connection
from django.db.utils import IntegrityError
from posthog.api.test.test_organization import create_organization
from posthog.api.test.test_team import create_team

from posthog.models import PersonOverride, PersonOverrideHelper, Team
from posthog.test.base import BaseTest


from django.db.utils import DEFAULT_DB_ALIAS, load_backend
from django.test.utils import CaptureQueriesContext

        PersonOverride.objects.all().delete()
        PersonOverrideHelper.objects.all().delete()

        with connection.cursor() as cursor:
            # Constraints are all deferred during normal execution, but for testing we want them to fail
            # during the test cases to properly assert exceptions raised by constraint failures.
            cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")

pytestmark = pytest.mark.django_db


        old_helper = PersonOverrideHelper.objects.create(
            team=self.team,
            uuid=old_person_id,
        )
        override_helper = PersonOverrideHelper.objects.create(
            team=self.team,
            uuid=override_person_id,
        )
        person_override = PersonOverride.objects.create(
            team=self.team,
            old_person_id=old_helper,
            override_person_id=override_helper,
            oldest_event=oldest_event,
            version=1,
        )
        person_override.save()

        assert person_override.old_person_id == old_helper
        assert person_override.override_person_id == override_helper

        new_override_helper = PersonOverrideHelper.objects.create(
            team=self.team,
            uuid=new_override_person_id,
        )

        with pytest.raises(IntegrityError):
            PersonOverride.objects.create(
                team=self.team,
                old_person_id=old_helper,
                override_person_id=new_override_helper,
                oldest_event=oldest_event,
                version=1,
            ).save()

    def test_person_override_same_old_person_id_in_different_teams(self):
        """Test a new old_person_id can match an existing from a different team."""
        oldest_event = dt.datetime.now(dt.timezone.utc)
        old_person_id = uuid4()
        override_person_id = uuid4()
        new_team = Team.objects.create(
            organization=self.organization,
            api_token="a different token",
        )

        old_helper = PersonOverrideHelper.objects.create(
            team=self.team,
            uuid=old_person_id,
        )
        override_helper = PersonOverrideHelper.objects.create(
            team=self.team,
            uuid=override_person_id,
        )

        p1 = PersonOverride.objects.create(
            team=self.team,
            old_person_id=old_helper,
            override_person_id=override_helper,
            oldest_event=oldest_event,
            version=1,
        )
        p1.save()

        assert p1.old_person_id == old_helper
        assert p1.override_person_id == override_helper

        new_team_old_helper = PersonOverrideHelper.objects.create(
            team=new_team,
            uuid=old_person_id,
        )
        new_team_override_helper = PersonOverrideHelper.objects.create(
            team=new_team,
            uuid=override_person_id,
        )

        p2 = PersonOverride.objects.create(
            team=new_team,
            old_person_id=new_team_old_helper,
            override_person_id=new_team_override_helper,
            oldest_event=oldest_event,
            version=1,
        )
        p2.save()

        assert p1.old_person_id.uuid == p2.old_person_id.uuid
        assert p1.override_person_id.uuid == p2.override_person_id.uuid
        assert p1.old_person_id.id != p2.old_person_id.id
        assert p1.override_person_id.id != p2.override_person_id.id
        assert p1.team != p2.team

    def test_person_override_disallows_override_person_id_as_old_person_id(self):
        """Test a new old_person_id cannot match an existing override_person_id.

        We re-use the override_person_id from the first model created as the old_person_id
        of the second model. We expect an exception on saving this second model.
        """
        oldest_event = dt.datetime.now(dt.timezone.utc)
        old_person_id = uuid4()
        override_person_id = uuid4()
        new_override_person_id = uuid4()

        old_helper = PersonOverrideHelper.objects.create(
            team=self.team,
            uuid=old_person_id,
        )
        override_helper = PersonOverrideHelper.objects.create(
            team=self.team,
            uuid=override_person_id,
        )

        person_override = PersonOverride.objects.create(
            team=self.team,
            old_person_id=old_helper,
            override_person_id=override_helper,
            oldest_event=oldest_event,
            version=1,
        )
        person_override.save()

        assert person_override.old_person_id == old_helper
        assert person_override.override_person_id == override_helper

        new_override_helper = PersonOverrideHelper.objects.create(
            team=self.team,
            uuid=new_override_person_id,
        )

        with pytest.raises(IntegrityError):
            PersonOverride.objects.create(
                team=self.team,
                old_person_id=override_helper,
                override_person_id=new_override_helper,
                oldest_event=oldest_event,
                version=1,
            ).save()

    def test_person_override_allows_override_person_id_as_old_person_id_in_different_teams(self):
        """Test a new old_person_id can match an override in a different team."""
        oldest_event = dt.datetime.now(dt.timezone.utc)
        old_person_id = uuid4()
        override_person_id = uuid4()
        new_override_person_id = uuid4()
        new_team = Team.objects.create(
            organization=self.organization,
            api_token="a much different token",
        )

        old_helper = PersonOverrideHelper.objects.create(
            team=self.team,
            uuid=old_person_id,
        )
        override_helper = PersonOverrideHelper.objects.create(
            team=self.team,
            uuid=override_person_id,
        )

        p1 = PersonOverride.objects.create(
            team=self.team,
            old_person_id=old_helper,
            override_person_id=override_helper,
            oldest_event=oldest_event,
            version=1,
        )
        p1.save()

        assert p1.old_person_id == old_helper
        assert p1.override_person_id == override_helper

        new_team_old_helper = PersonOverrideHelper.objects.create(
            team=new_team,
            uuid=override_person_id,
        )
        new_team_override_helper = PersonOverrideHelper.objects.create(
            team=new_team,
            uuid=new_override_person_id,
        )
        p2 = PersonOverride.objects.create(
            team=new_team,
            old_person_id=new_team_old_helper,
            override_person_id=new_team_override_helper,
            oldest_event=oldest_event,
            version=1,
        ).save()


def test_person_override_allow_same_old_person_id_in_different_teams():
    """Test a new old_person_id can match an existing from a different team."""
    organization = create_organization(name="test")
    team = create_team(organization=organization)

    oldest_event = dt.datetime.now(dt.timezone.utc)
    old_person_id = uuid4()
    override_person_id = uuid4()
    new_team = Team.objects.create(
        organization=organization,
        api_token="a different token",
    )

    Person.objects.create(
        team_id=team.pk,
        uuid=override_person_id,
    )

    p1 = PersonOverride.objects.create(
        team=team,
        old_person_id=old_person_id,
        override_person_id=override_person_id,
        oldest_event=oldest_event,
        version=1,
    )
    p1.save()

    assert p1.old_person_id == old_person_id
    assert p1.override_person_id == override_person_id

    p2 = PersonOverride.objects.create(
        team=new_team,
        old_person_id=old_person_id,
        override_person_id=override_person_id,
        oldest_event=oldest_event,
        version=1,
    )
    p2.save()

    assert p1.old_person_id == p2.old_person_id
    assert p1.override_person_id == p2.override_person_id
    assert p1.team != p2.team


def test_person_override_allows_override_person_id_as_old_person_id_in_different_teams():
    """Test a new old_person_id can match an override in a different team."""
    organization = create_organization(name="test")
    team = create_team(organization=organization)

    oldest_event = dt.datetime.now(dt.timezone.utc)
    old_person_id = uuid4()
    override_person_id = uuid4()
    new_override_person_id = uuid4()
    new_team = Team.objects.create(
        organization=organization,
        api_token="a much different token",
    )

    Person.objects.create(
        team_id=team.pk,
        uuid=override_person_id,
    )

    p1 = PersonOverride.objects.create(
        team=team,
        old_person_id=old_person_id,
        override_person_id=override_person_id,
        oldest_event=oldest_event,
        version=1,
    )
    p1.save()

    assert p1.old_person_id == old_person_id
    assert p1.override_person_id == override_person_id

    Person.objects.create(
        team_id=team.pk,
        uuid=new_override_person_id,
    )

    p2 = PersonOverride.objects.create(
        team=new_team,
        old_person_id=override_person_id,
        override_person_id=new_override_person_id,
        oldest_event=oldest_event,
        version=1,
    )
    p2.save()

    assert p1.override_person_id == p2.old_person_id
    assert p2.override_person_id == new_override_person_id
    assert p1.team != p2.team


def test_person_override_creation_disallowed_for_non_existing_person():
    """This is guaranteed by the foreign key constraint."""
    organization = create_organization(name="test")
    team = create_team(organization=organization)

    oldest_event = dt.datetime.now(dt.timezone.utc)
    person_id = uuid4()

    Person.objects.create(
        team_id=team.pk,
        uuid=person_id,
    )

    with pytest.raises(IntegrityError):
        PersonOverride.objects.create(
            team=team,
            old_person_id=person_id,
            override_person_id=person_id,
            oldest_event=oldest_event,
            version=0,
        ).save()


def test_person_override_allows_duplicate_override_person_id():
    """Test duplicate override_person_ids with different old_person_ids are allowed."""
    organization = create_organization(name="test")
    team = create_team(organization=organization)

    oldest_event = dt.datetime.now(dt.timezone.utc)
    override_person_id = uuid4()
    n_person_overrides = 2
    created = []

    Person.objects.create(uuid=override_person_id, team=team)

    for _ in range(n_person_overrides):
        old_person_id = uuid4()

        person_override = PersonOverride.objects.create(
            team=team,
            old_person_id=old_person_id,
            override_person_id=override_person_id,
            oldest_event=oldest_event,
            version=1,
        )

        assert p1.override_person_id.uuid == p2.old_person_id.uuid
        assert p2.override_person_id == new_team_override_helper
        assert p1.team != p2.team

        second_cursor.execute(
            """
                DELETE FROM posthog_person WHERE uuid = %s
            """,
            [override_person_id],
        )

        second_cursor.execute(
            """
                INSERT INTO posthog_personoverride
                (team_id, old_person_id, override_person_id, oldest_event, version)
                VALUES
                (%s, %s, %s, %s, %s)
            """,
            [team.pk, override_person_id, new_override_person_id, oldest_event, 1],
        )

        first_cursor.execute("COMMIT")

        created.append(person_override)

    assert all(p.override_person_id == override_person_id for p in created)
    assert len(set(p.old_person_id for p in created)) == n_person_overrides


def test_person_override_allows_old_person_id_as_override_person_id_in_different_teams():
    """Test a new override_person_id can match an old in a different team."""
    organization = create_organization(name="test")
    team = create_team(organization=organization)

    oldest_event = dt.datetime.now(dt.timezone.utc)
    old_person_id = uuid4()
    override_person_id = uuid4()
    new_old_person_id = uuid4()
    new_team = Team.objects.create(
        organization=organization,
        api_token="a significantly different token",
    )

    Person.objects.create(uuid=old_person_id, team=team)
    Person.objects.create(uuid=override_person_id, team=team)
    Person.objects.create(uuid=new_old_person_id, team=team)

    p1 = PersonOverride.objects.create(
        team=team,
        old_person_id=old_person_id,
        override_person_id=override_person_id,
        oldest_event=oldest_event,
        version=1,
    )
    p1.save()

    assert p1.old_person_id == old_person_id
    assert p1.override_person_id == override_person_id

    p2 = PersonOverride.objects.create(
        team=new_team,
        old_person_id=new_old_person_id,
        override_person_id=old_person_id,
        oldest_event=oldest_event,
        version=1,
    )
    p2.save()

    assert p1.old_person_id == p2.override_person_id
    assert p2.old_person_id == new_old_person_id
    assert p1.team != p2.team


@pytest.mark.django_db(transaction=True)
def test_person_deletion_disallowed_when_override_exists():
    """Person deletion would result in an error if the override exists"""
    organization = create_organization(name="test")
    team = create_team(organization=organization)

    oldest_event = dt.datetime.now(dt.timezone.utc)
    old_person_id = uuid4()
    override_person_id = uuid4()

    override_person = Person.objects.create(
        team_id=team.pk,
        uuid=override_person_id,
    )
    PersonOverride.objects.create(
        team=team,
        old_person_id=old_person_id,
        override_person_id=override_person_id,
        oldest_event=oldest_event,
        version=0,
    ).save()

    with pytest.raises(IntegrityError):
        override_person.delete()


"""
Concurrency tests for person overrides table
Goal: verify that we don't end up in a situation with the same uuid is both
an old person id and an override person id

- there are two cases that we want to check for
    - concurrent merges
    - concurrent merge and person deletion

In both cases one of the transactions will wait on the lock,
so they can only complete in one order (which is tested below).

Note that to test the race condition scenario we need to:
    1. create multiple concurrent transactions, such that we can verify
    constraints are enforced at COMMIT time.
    2. enable transactions for the Django test. This is more so we can see data
    from the main Django PostgreSQL connection session in the other
    concurrent transactions. Not 100% required but makes things a little
    easier to write.
"""


@pytest.mark.django_db(transaction=True)
def test_person_override_allow_merge_first_then_delete():
    # This essentially just verifies our merge and delete functions work as expected
    organization = create_organization(name="test")
    team = create_team(organization=organization)

    oldest_event = dt.datetime.now(dt.timezone.utc)
    old_person_id = uuid4()
    override_person_id = uuid4()

    Person.objects.create(uuid=old_person_id, team=team)
    Person.objects.create(uuid=override_person_id, team=team)
    with create_connection() as merge_cursor:
        merge_cursor.execute("BEGIN")
        _merge_people(team, merge_cursor, old_person_id, override_person_id, oldest_event)
        merge_cursor.execute("COMMIT")

    assert list(PersonOverride.objects.all().values_list("old_person_id", "override_person_id")) == [
        (old_person_id, override_person_id),
    ]  # type: ignore

    with create_connection() as delete_cursor:
        delete_cursor.execute("BEGIN")
        _delete_person(team, delete_cursor, override_person_id)
        delete_cursor.execute("COMMIT")

    assert list(PersonOverride.objects.filter(team=team).all()) == []  # type: ignore


@pytest.mark.django_db(transaction=True)
def test_person_override_disallow_merge_if_delete_ran_concurrently():
    organization = create_organization(name="test")
    team = create_team(organization=organization)

    oldest_event = dt.datetime.now(dt.timezone.utc)
    old_person_id = uuid4()
    override_person_id = uuid4()

    Person.objects.create(uuid=old_person_id, team=team)
    Person.objects.create(uuid=override_person_id, team=team)
    with create_connection() as merge_cursor, create_connection() as delete_cursor:
        # each transaction gets a "copy" of the DB state
        merge_cursor.execute("BEGIN")
        delete_cursor.execute("BEGIN")
        # merge and delete
        _merge_people(team, merge_cursor, old_person_id, override_person_id, oldest_event)
        _delete_person(team, delete_cursor, override_person_id)

        # finish delete first, then merge fails
        delete_cursor.execute("COMMIT")
        with pytest.raises(IntegrityError):
            merge_cursor.execute("COMMIT")

        old_helper = PersonOverrideHelper.objects.create(
            team=self.team,
            uuid=old_person_id,
        )
        old_helper.save()

        override_helper = PersonOverrideHelper.objects.create(
            team=self.team,
            uuid=override_person_id,
        )
        override_helper.save()

        person_override = PersonOverride.objects.create(
            team=self.team,
            old_person_id=old_helper,
            override_person_id=override_helper,
            oldest_event=oldest_event,
            version=1,
        )
        person_override.save()

        assert person_override.old_person_id == old_helper
        assert person_override.override_person_id == override_helper

        new_old_helper = PersonOverrideHelper.objects.create(
            team=self.team,
            uuid=new_old_person_id,
        )
        new_old_helper.save()

        with pytest.raises(IntegrityError):
            p = PersonOverride.objects.create(
                team=self.team,
                old_person_id=new_old_helper,
                override_person_id=old_helper,
                oldest_event=oldest_event,
                version=1,
            )
            p.save()

    def test_person_override_old_person_id_as_override_person_id_in_different_teams(self):
        """Test a new override_person_id can match an old in a different team."""
        oldest_event = dt.datetime.now(dt.timezone.utc)
        old_person_id = uuid4()
        override_person_id = uuid4()
        new_old_person_id = uuid4()
        new_team = Team.objects.create(
            organization=self.organization,
            api_token="a significantly different token",
        )

        old_helper = PersonOverrideHelper.objects.create(
            team=self.team,
            uuid=old_person_id,
        )
        override_helper = PersonOverrideHelper.objects.create(
            team=self.team,
            uuid=override_person_id,
        )

        p1 = PersonOverride.objects.create(
            team=self.team,
            old_person_id=old_helper,
            override_person_id=override_helper,
            oldest_event=oldest_event,
            version=1,
        )
        p1.save()

        assert p1.old_person_id == old_helper
        assert p1.override_person_id == override_helper

        new_old_helper = PersonOverrideHelper.objects.create(
            team=new_team,
            uuid=new_old_person_id,
        )
        new_override_helper = PersonOverrideHelper.objects.create(
            team=new_team,
            uuid=old_helper.uuid,
        )

        p2 = PersonOverride.objects.create(
            team=new_team,
            old_person_id=new_old_helper,
            override_person_id=new_override_helper,
            oldest_event=oldest_event,
            version=1,
        )
        p2.save()

        assert p1.old_person_id.uuid == p2.override_person_id.uuid
        assert p1.old_person_id.team == p1.override_person_id.team
        assert p2.old_person_id == new_old_helper
        assert p1.team != p2.team

    def test_person_override_allows_duplicate_override_person_id(self):
        """Test duplicate override_person_ids with different old_person_ids are allowed."""
        oldest_event = dt.datetime.now(dt.timezone.utc)
        override_person_id = uuid4()
        n_person_overrides = 2
        created = []

        override_helper = PersonOverrideHelper.objects.create(
            team=self.team,
            uuid=override_person_id,
        )

        for _ in range(n_person_overrides):
            old_person_id = uuid4()
            old_helper = PersonOverrideHelper.objects.create(
                team=self.team,
                uuid=old_person_id,
            )

            person_override = PersonOverride.objects.create(
                team=self.team,
                old_person_id=old_helper,
                override_person_id=override_helper,
                oldest_event=oldest_event,
                version=1,
            )
            person_override.save()

            created.append(person_override)

        assert all(p.override_person_id == override_helper for p in created)
        assert len(set(p.old_person_id.uuid for p in created)) == n_person_overrides
