# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.utils.timezone
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='SavedSearch',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=100)),
                ('url', models.CharField(max_length=200, db_index=True)),
                ('querystring', models.TextField(blank=True)),
                ('default', models.BooleanField(default=False)),
                ('date_created', models.DateTimeField(default=django.utils.timezone.now, editable=False)),
                ('user', models.ForeignKey(to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'ordering': (b'name',),
                'verbose_name_plural': b'saved searches',
            },
            bases=(models.Model,),
        ),
    ]
