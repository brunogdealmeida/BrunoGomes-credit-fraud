FROM apache/superset:3.1.3

USER root

# Install without deps to avoid upgrading SQLAlchemy/alembic that Superset depends on
RUN pip install --no-cache-dir --no-deps sqlalchemy-dremio

# Superset initialization script used as the entry point
COPY docker-init.sh /docker-init.sh
RUN chmod +x /docker-init.sh

USER superset

ENTRYPOINT ["/bin/sh", "/docker-init.sh"]
