#!/usr/bin/env bash

# Читаем пароль из файла секрета
ADMIN_PASSWORD=$(cat /run/secrets/clickhouse_admin)
USER_PASSWORD=$(cat /run/secrets/clickhouse_user)

# Формируем XML-конфигурацию для пользователей
cat > /etc/clickhouse-server/users.d/users.xml << EOL
<clickhouse>
    <users>
        <default>
            <password>${ADMIN_PASSWORD}</password>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </default>

        <admin>
            <password>${ADMIN_PASSWORD}</password>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </admin>

        <user>
            <password>${USER_PASSWORD}</password>
            <profile>default</profile>
            <quota>default</quota>
        </user>
    </users>
</clickhouse>
EOL