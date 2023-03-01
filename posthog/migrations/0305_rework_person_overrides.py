from django.db import migrations, models


# no-op change
class Migration(migrations.Migration):

    dependencies = [
        ("posthog", "0304_store_dashboard_template_in_db"),
    ]

    operations = [
        migrations.AddField(
            model_name="team",
            name="session_recording_version_2",
            field=models.CharField(blank=True, max_length=24, null=True),
        ),
    ]
