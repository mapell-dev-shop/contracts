# Changing an event

Consumers ignore fields they do not know, and that rule decides which changes are safe.

* Adding an optional field is compatible. Add it to the schema, keep the version, then start publishing it.
* Anything else is breaking: removing or renaming a field, changing its type or meaning, or making it required. Add a new schema file `<event>.v2.schema.json` and a new topic ending in `.v2`, publish both versions until every consumer has moved, then retire version 1.
* Never edit a published schema file in a breaking way. Consumers that have not moved to the new version still read it.

Topic names follow `shop.<owning context>.<event in kebab case>.v<major>`.
