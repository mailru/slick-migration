-- versions of avatar's levels
CREATE TABLE "gm.avatar.level.version" (
  "VersionId" SERIAL NOT NULL PRIMARY KEY,
  "VersionStart" TIMESTAMP NOT NULL,
  "VersionEnd" TIMESTAMP DEFAULT TIMESTAMP  '9999-01-01 00:00:00.0' NOT NULL,
  "EntityId" BIGINT NOT NULL,
  "Level" INTEGER NOT NULL);

COMMENT ON TABLE "gm.avatar.level.version" IS 'Версии свойства avatar.level';

CREATE INDEX "versionStartIndex" ON "gm.avatar.level.version" ("VersionStart");
CREATE INDEX "versionEndIndex" ON "gm.avatar.level.version" ("VersionEnd");
CREATE INDEX "entityIdIndex" ON "gm.avatar.level.version" ("EntityId");

CREATE OR REPLACE FUNCTION VersionsBeforeInsert() RETURNS TRIGGER AS $beforeInsert$
    DECLARE versionEnd TIMESTAMP;
    DECLARE newVersionStartLiteral TEXT;
    DECLARE tableNameLiteral TEXT;

    BEGIN
        tableNameLiteral := quote_ident( TG_TABLE_NAME);

        IF (TG_OP = 'DELETE') THEN
            RAISE EXCEPTION 'Version DELETE has not been implemented yet.';
        ELSIF (TG_OP = 'UPDATE') THEN
            RAISE EXCEPTION 'Version UPDATE has not been implemented yet.';
        ELSIF (TG_OP = 'INSERT') THEN
            newVersionStartLiteral := quote_literal(NEW."VersionStart");

            -- look up for old version end. If exists then we are modifying history and should retain subsequent versions.
            EXECUTE
              'SELECT "VersionEnd" FROM ' || tableNameLiteral ||
              ' WHERE "VersionStart" <= $1
                  AND "VersionEnd"   >  $1
                  AND "EntityId" = $2'
              INTO versionEnd
              USING NEW."VersionStart", NEW."EntityId";

            IF versionEnd IS NOT NULL THEN
              NEW."VersionEnd" := versionEnd;
                -- updating previous version end moment so that it is adjacent with the new version.
                EXECUTE
                  'UPDATE ' || tableNameLiteral ||
                  ' SET   "VersionEnd"    = '|| newVersionStartLiteral ||
                  ' WHERE "VersionStart" <= '|| newVersionStartLiteral ||
                  '   AND "VersionEnd"   >  '|| newVersionStartLiteral ||
                  '   AND "EntityId"     =  '|| quote_literal(NEW."EntityId") ||
                  ';'
                ;

            ELSE -- END IF;IF versionEnd IS NULL THEN  -- нет старой версии, которую надо модифицировать.
              -- the current version is absent. However, we may insert before the first version. Let's check this case.
              EXECUTE
                'SELECT     "VersionStart" FROM ' || tableNameLiteral ||
                ' WHERE     "VersionStart" > $1
                  AND       "EntityId" = $2
                  ORDER BY  "VersionStart"
                  LIMIT 1'
                INTO versionEnd
                USING NEW."VersionStart", NEW."EntityId";  -- вызов запроса с 2-мя аргументами

              IF versionEnd IS NOT NULL THEN
                NEW."VersionEnd" := versionEnd;
              ELSE -- versionEnd IS NULL
                NEW."VersionEnd" := '9999-01-01 00:00:00'::TIMESTAMP; -- if default value works then this line can be removed.
              END IF;
            END IF;
            RETURN NEW;
        END IF;
        RETURN NULL;
    END;
$beforeInsert$ LANGUAGE plpgsql;

CREATE TRIGGER beforeInsertTrigger
 BEFORE INSERT ON "gm.avatar.level.version"
 FOR EACH ROW
 EXECUTE PROCEDURE VersionsBeforeInsert();


