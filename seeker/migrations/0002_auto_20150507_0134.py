# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.conf import settings
from django.db import migrations, models
from seeker import get_app_label


class Migration(migrations.Migration):

    dependencies = [
        (get_app_label(), '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='savedsearch',
            name='user',
            field=models.ForeignKey(related_name='seeker_searches', to=settings.AUTH_USER_MODEL, on_delete=models.CASCADE),
        ),
    ]
