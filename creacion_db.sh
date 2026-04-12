#!/bin/bash

#config inicial
DB_NAME="indicadores_gt"
DB_PATH="/var/lib/firebird/data/${DB_NAME}.fdb"
SQL_SOURCE="$HOME/Documentos/Proyectos/Proyecto2-BD1/modelo_fisico_firebird.sql"
SQL_TARGET="/var/lib/firebird/modelo_fisico_firebird.sql"
USER="SYSDBA"
PASS="masterkey"

#validaciones
echo "Base de datos: $DB_NAME"
echo "Ruta DB: $DB_PATH"
echo "SQL origen: $SQL_SOURCE"
echo "SQL destino: $SQL_TARGET"

if [ ! -f "$SQL_SOURCE" ]; then
  echo "Error: no existe el archivo SQL en:"
  echo "$SQL_SOURCE"
  exit 1
fi

#crear dirs si no exosten
sudo mkdir -p /var/lib/firebird/data
sudo mkdir -p /tmp/firebird
sudo chown -R firebird:firebird /var/lib/firebird
sudo chown firebird:firebird /tmp/firebird
sudo chmod 750 /tmp/firebird


#eliminar db si existe
if sudo test -e "$DB_PATH"; then
  echo "Eliminando db existente..."
  sudo systemctl stop firebird
  sudo rm -f "$DB_PATH"
  sudo systemctl start firebird
else
  echo "No existe db anterior"
fi

#copiar sql a ruta accesible
echo "Copiando script SQL..."
sudo cp "$SQL_SOURCE" "$SQL_TARGET"
sudo chown firebird:firebird "$SQL_TARGET"
sudo chmod 640 "$SQL_TARGET"

# crear db
echo "Creando db..."
sudo -u firebird isql-fb <<EOF
CREATE DATABASE '$DB_PATH'
USER '$USER' PASSWORD '$PASS';
COMMIT;
QUIT;
EOF

if ! sudo test -e "$DB_PATH"; then
  echo "e: no se pudo crear la db"
  exit 1
fi

echo "DB creada"

# ejecutar modelo fisico
echo "Ejecutando modelo físico..."
sudo -u firebird isql-fb -user "$USER" -password "$PASS" "$DB_PATH" -i "$SQL_TARGET"

#verificacion final
echo "Verificando tablas creadas..."
sudo -u firebird isql-fb -user "$USER" -password "$PASS" "$DB_PATH" <<EOF
SHOW TABLES;
QUIT;
EOF

echo "Script cargado"