PATH := node_modules/.bin:$(PATH)

define EXPAND_EXPORTS
export $(word 1, $(subst =, , $(1))) := $(word 2, $(subst =, , $(1)))
endef

# expand PG* environment vars
$(foreach a,$(shell set -a; node_modules/.bin/pgexplode),$(eval $(call EXPAND_EXPORTS,$(a))))

define create_relation
@psql -v ON_ERROR_STOP=1 -qXc "\d $(subst db/,,$@)" > /dev/null 2>&1 || \
	psql -v ON_ERROR_STOP=1 -qX1f sql/$(subst db/,,$@).sql
endef

define register_relation_target
.PHONY: db/$(strip $(1))

db/$(strip $(1)): db
	$$(call create_relation)
endef

$(foreach fn,$(shell ls sql/ 2> /dev/null | sed 's/\..*//'),$(eval $(call register_relation_target,$(fn))))

.PHONY: DATABASE_URL

DATABASE_URL:
	@test "${$@}" || (echo "$@ is undefined" && false)

.PHONY: db

db: DATABASE_URL
	@psql -c "SELECT 1" > /dev/null 2>&1 || \
	createdb

db/all: db/changesets db/changesets_countries db/countries db/users
