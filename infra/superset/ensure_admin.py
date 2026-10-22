"""Idempotent configured-admin creation without resetting other accounts."""
import os
from superset.app import create_app
from superset.extensions import appbuilder

app=create_app()
with app.app_context():
    sm=appbuilder.sm
    name=os.environ['SUPERSET_ADMIN_USERNAME']
    user=sm.find_user(username=name)
    if user is None:
        user=sm.add_user(name,'DSP391m','EDA',os.environ['SUPERSET_ADMIN_EMAIL'],sm.find_role('Admin'),os.environ['SUPERSET_ADMIN_PASSWORD'])
        if not user:
            raise RuntimeError('Unable to create configured administrator')
        print('Configured EDA administrator created.')
    else:
        print('Configured administrator already exists; preserved account and password.')
