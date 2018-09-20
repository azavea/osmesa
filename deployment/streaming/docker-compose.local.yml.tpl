version: '3.0'
services:
  db:
    image: postgres:10.5
    volumes:
      - ../sql:/docker-entrypoint-initdb.d
    environment:
      - POSTGRES_PASSWORD=pgsecret
    networks:
      db:
        aliases:
          - database
  changeset-stream:
    image: ${LOCAL_IMG}
    volumes:
      - ~/.aws:/root/.aws
      - ~/.ssh:/root/.ssh
    command: >
      /spark/bin/spark-submit --class osmesa.analytics.oneoffs.ChangesetStreamProcessor /opt/osmesa-analytics.jar
      --changeset-source ${CHANGESET_SOURCE}
      --start-sequence ${CHANGESET_START}
      --database-uri postgresql://postgres:pgsecret@database:5432/postgres
    networks:
      - db
  augdiff-stream:
    image: ${LOCAL_IMG}
    volumes:
      - ~/.aws:/root/.aws
      - ~/.ssh:/root/.ssh
    environment:
      - AWS_PROFILE=gaasdg
    command: >
      /spark/bin/spark-submit --class osmesa.analytics.oneoffs.AugmentedDiffStreamProcessor /opt/osmesa-analytics.jar
      --augmented-diff-source ${AUGDIFF_SOURCE}
      --start-sequence ${AUGDIFF_START}
      --database-uri postgresql://postgres:pgsecret@database:5432/postgres
    volumes:
      - ~/.aws:/root/.aws
    networks:
      - db
  change-stream:
    image: ${LOCAL_IMG}
    command: >
      /spark/bin/spark-submit --class osmesa.analytics.oneoffs.ChangeStreamProcessor /opt/osmesa-analytics.jar
      --change-source ${CHANGE_SOURCE}
      --start-sequence ${CHANGE_START}
      --database-uri postgresql://postgres:pgsecret@database:5432/postgres
    networks:
      - db
networks:
  db:

