from django.db import migrations


# no-op change
class Migration(migrations.Migration):

    dependencies = [
        ("posthog", "0304_store_dashboard_template_in_db"),
    ]

    operations = []  # type: ignore
