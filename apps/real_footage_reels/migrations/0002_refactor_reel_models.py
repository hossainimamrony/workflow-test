# Generated manually for real_footage_reels refactor

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("real_footage_reels", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="ReelRun",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("run_id", models.CharField(max_length=64, unique=True)),
                ("listing_title", models.CharField(blank=True, max_length=255)),
                ("stock_id", models.CharField(blank=True, max_length=128)),
                ("car_description", models.TextField(blank=True)),
                ("listing_price", models.CharField(blank=True, max_length=128)),
                ("status", models.CharField(default="created", max_length=32)),
                ("report", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"ordering": ["-created_at"]},
        ),
        migrations.RemoveField(
            model_name="reelrenderjob",
            name="source_name",
        ),
        migrations.RemoveField(
            model_name="reelrenderjob",
            name="notes",
        ),
        migrations.AddField(
            model_name="reelrenderjob",
            name="job_id",
            field=models.CharField(default="", max_length=64, unique=True),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="reelrenderjob",
            name="command",
            field=models.CharField(default="prepare", max_length=32),
        ),
        migrations.AddField(
            model_name="reelrenderjob",
            name="payload",
            field=models.JSONField(blank=True, default=dict),
        ),
        migrations.AddField(
            model_name="reelrenderjob",
            name="result",
            field=models.JSONField(blank=True, default=dict),
        ),
        migrations.AddField(
            model_name="reelrenderjob",
            name="error",
            field=models.TextField(blank=True),
        ),
        migrations.AddField(
            model_name="reelrenderjob",
            name="started_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="reelrenderjob",
            name="finished_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
