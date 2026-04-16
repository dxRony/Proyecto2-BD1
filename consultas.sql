--1. Cantidad de homicidios registrados por año y departamento.
SELECT FIRST 10
    f.anio AS anio,
    dpto.nombre AS departamento,
    COUNT(*) AS cantidad_homicidios
FROM hecho_delictivo hecho
JOIN delito d
    ON d.id = hecho.id_delito
JOIN fecha f
    ON f.id = hecho.id_fecha
JOIN municipio m
    ON m.id = hecho.id_municipio
JOIN departamento dpto
    ON dpto.id = m.id_departamento
WHERE UPPER(d.nombre) LIKE '%HOMICID%'
GROUP BY
    f.anio,
    dpto.nombre
ORDER BY
    f.anio,
    cantidad_homicidios DESC,
    dpto.nombre;

--2. Número de denuncias por violencia contra la mujer por municipio.
SELECT FIRST 10
    m.nombre AS municipio,
    dpto.nombre AS departamento,
    SUM(dcvme.cantidad) AS total_denuncias
FROM delito_contra_vida_mujer_estadistica dcvme
JOIN municipio m
    ON m.id = dcvme.id_municipio
JOIN departamento dpto
    ON dpto.id = dcvme.id_departamento
GROUP BY
    m.nombre,
    dpto.nombre
ORDER BY
    total_denuncias DESC,
    dpto.nombre,
    m.nombre;

--3. Top 5 tipos de hechos delictivos más frecuentes en los últimos 5 años.
SELECT FIRST 5
    d.nombre AS delito,
    COUNT(*) AS total_hechos
FROM hecho_delictivo hecho
JOIN delito d
    ON d.id = hecho.id_delito
JOIN fecha f
    ON f.id = hecho.id_fecha
WHERE f.anio >= EXTRACT(YEAR FROM CURRENT_DATE) - 4
GROUP BY
    d.nombre
ORDER BY
    total_hechos DESC,
    d.nombre;

--4. Sentencias dictadas por tipo de delito y año.
SELECT FIRST 20
    f.anio AS anio,
    d.nombre AS delito,
    COUNT(s.id) AS total_sentencias
FROM sentencia s
JOIN proceso_judicial pj
    ON pj.id = s.id_proceso_judicial
JOIN delito d
    ON d.id = pj.id_delito
JOIN hecho_delictivo hecho
    ON hecho.id = pj.id_hecho_delictivo
JOIN fecha f
    ON f.id = hecho.id_fecha
GROUP BY
    f.anio,
    d.nombre
ORDER BY
    f.anio,
    total_sentencias DESC,
    d.nombre;

--5. Promedio de edad de las víctimas de violencia intrafamiliar.
SELECT
    AVG(p.edad) AS promedio_edad_victimas
FROM violencia_intrafamiliar vi
JOIN persona p
    ON p.id = vi.id_persona_victima
WHERE p.edad IS NOT NULL;

--6. Distribución de embarazos adolescentes (menores de 19 años) por región.
SELECT
    d.nombre AS departamento,
    SUM(rs.cantidad) AS total_embarazos_adolescentes
FROM registro_salud rs
JOIN tipo_indicador_salud tis
    ON tis.id = rs.id_tipo_indicador_salud
JOIN grupo_etario ge
    ON ge.id = rs.id_grupo_etario
JOIN municipio m
    ON m.id = rs.id_municipio
JOIN departamento d
    ON d.id = m.id_departamento
WHERE UPPER(tis.nombre) = UPPER('Embarazos adolescentes')
  AND ge.edad_max < 19
GROUP BY
    d.nombre
ORDER BY
    total_embarazos_adolescentes DESC,
    d.nombre;

--7. Cantidad de casos de violencia infantil relacionados con trabajo infantil.
SELECT
    COUNT(DISTINCT ti.id_persona) AS cantidad_casos_relacionados
FROM trabajo_infantil ti
JOIN persona p
    ON p.id = ti.id_persona
JOIN involucramiento_hecho ih
    ON ih.id_persona = ti.id_persona
WHERE p.edad < 18;

--8. Porcentaje de denuncias por discriminación según etnia.
SELECT
    COALESCE(ge.nombre, 'Sin etnia registrada') AS grupo_etnico,
    COUNT(*) AS total_casos,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        2
    ) AS porcentaje
FROM caso_discriminacion cd
LEFT JOIN grupo_etnico ge
    ON ge.id = cd.id_grupo_etnico
GROUP BY
    ge.nombre
ORDER BY
    porcentaje DESC,
    grupo_etnico;

--9. Comparativa entre nivel de escolaridad y tipo de falta judicial cometida.
SELECT
    COALESCE(e.nombre, 'Sin escolaridad registrada') AS escolaridad,
    tf.nombre AS tipo_falta,
    COUNT(*) AS cantidad_casos
FROM falta_judicial fj
LEFT JOIN escolaridad e
    ON e.id = fj.id_escolaridad
JOIN tipo_falta tf
    ON tf.id = fj.id_tipo_falta
GROUP BY
    e.nombre,
    tf.nombre
ORDER BY
    escolaridad,
    cantidad_casos DESC,
    tipo_falta;

--10. Número de necropsias realizadas por año.
SELECT
    f.anio AS anio,
    COUNT(*) AS total_necropsias
FROM necropsia n
JOIN fecha f
    ON f.id = n.id_fecha_ingreso
GROUP BY f.anio
ORDER BY f.anio;

--11. Distribución de casos de violencia estructural por tipo de discriminación
SELECT
    td.nombre AS tipo_discriminacion,
    COUNT(*) AS total_casos,
    ROUND(
        COUNT(*) * 100.0 / (SELECT COUNT(*) FROM caso_discriminacion),
        2
    ) AS porcentaje
FROM caso_discriminacion cd
JOIN tipo_discriminacion td
    ON td.id = cd.id_tipo_discriminacion
GROUP BY
    td.nombre
ORDER BY
    porcentaje DESC,
    td.nombre;

--12. Relación entre ocupación y faltas judiciales registradas
SELECT
    COALESCE(o.nombre, 'Sin ocupación registrada') AS ocupacion,
    COUNT(*) AS total_faltas
FROM falta_judicial fj
LEFT JOIN ocupacion o
    ON o.id = fj.id_ocupacion
GROUP BY
    o.nombre
ORDER BY
    total_faltas DESC,
    ocupacion;

--13. Casos de violencia intrafamiliar contra mujeres con sentencia firme
SELECT
    tf.nombre AS tipo_fallo,
    COUNT(*) AS total_sentencias
FROM violencia_intrafamiliar vi
JOIN persona p
    ON p.id = vi.id_persona_victima
JOIN sexo s
    ON s.id = p.id_sexo
JOIN proceso_judicial pj
    ON pj.id_hecho_delictivo = vi.id_hecho_delictivo
JOIN sentencia se
    ON se.id_proceso_judicial = pj.id
JOIN tipo_fallo tf
    ON tf.id = se.id_tipo_fallo
WHERE s.codigo = 'M'
GROUP BY
    tf.nombre
ORDER BY
    total_sentencias DESC,
    tf.nombre;

--14. Idiomas hablados por víctimas de discriminación.
SELECT
    COALESCE(cl.nombre, 'Sin comunidad lingüística registrada') AS idioma_o_comunidad,
    COUNT(*) AS total_victimas
FROM caso_discriminacion cd
LEFT JOIN comunidad_linguistica cl
    ON cl.id = cd.id_comunidad_linguistica
GROUP BY
    cl.nombre
ORDER BY
    total_victimas DESC,
    idioma_o_comunidad;

--15. Número de casos de violencia intrafamiliar con reiteración de denuncia
SELECT
    CASE
        WHEN vi.reiteracion_denuncia = TRUE THEN 'Con reiteración'
        WHEN vi.reiteracion_denuncia = FALSE THEN 'Sin reiteración'
        ELSE 'Sin dato'
    END AS estado_reiteracion,
    COUNT(*) AS total_casos
FROM violencia_intrafamiliar vi
GROUP BY
    CASE
        WHEN vi.reiteracion_denuncia = TRUE THEN 'Con reiteración'
        WHEN vi.reiteracion_denuncia = FALSE THEN 'Sin reiteración'
        ELSE 'Sin dato'
    END
ORDER BY
    total_casos DESC;

--16. Tasa de denuncias de violencia infantil en población escolarizada vs. no escolarizada.
SELECT
    CASE
        WHEN ti.asiste_escuela = TRUE THEN 'Escolarizada'
        WHEN ti.asiste_escuela = FALSE THEN 'No escolarizada'
        ELSE 'Sin dato'
    END AS condicion_escolar,
    COUNT(*) AS total_casos,
    ROUND(
        COUNT(*) * 100.0 / (SELECT COUNT(*) FROM trabajo_infantil),
        2
    ) AS porcentaje
FROM trabajo_infantil ti
GROUP BY
    CASE
        WHEN ti.asiste_escuela = TRUE THEN 'Escolarizada'
        WHEN ti.asiste_escuela = FALSE THEN 'No escolarizada'
        ELSE 'Sin dato'
    END
ORDER BY
    total_casos DESC;

--17. Casos de trabajo infantil por sector económico (agricultura, industria, etc.).
SELECT
    COALESCE(se.nombre, 'Sin sector registrado') AS sector_economico,
    COUNT(*) AS total_casos
FROM trabajo_infantil ti
LEFT JOIN sector_economico se
    ON se.id = ti.id_sector_economico
GROUP BY
    se.nombre
ORDER BY
    total_casos DESC,
    sector_economico;

--18. Comparativa entre edad promedio y tipo de violencia sufrida (infantil, intrafamiliar, estructural, etc.).
SELECT
    'Violencia intrafamiliar' AS tipo_violencia,
    AVG(p.edad) AS promedio_edad
FROM violencia_intrafamiliar vi
JOIN persona p
    ON p.id = vi.id_persona_victima
WHERE p.edad IS NOT NULL
UNION ALL
SELECT
    'Violencia estructural' AS tipo_violencia,
    AVG(p.edad) AS promedio_edad
FROM caso_discriminacion cd
JOIN persona p
    ON p.id = cd.id_persona
WHERE p.edad IS NOT NULL
UNION ALL
SELECT
    'Trabajo infantil' AS tipo_violencia,
    AVG(p.edad) AS promedio_edad
FROM trabajo_infantil ti
JOIN persona p
    ON p.id = ti.id_persona
WHERE p.edad IS NOT NULL;

--19. Casos de desnutrición aguda por departamento, municipio y año.
SELECT
    d.nombre AS departamento,
    m.nombre AS municipio,
    f.anio AS anio,
    SUM(rs.cantidad) AS total_casos
FROM registro_salud rs
JOIN tipo_indicador_salud tis
    ON tis.id = rs.id_tipo_indicador_salud
JOIN municipio m
    ON m.id = rs.id_municipio
JOIN departamento d
    ON d.id = m.id_departamento
JOIN fecha f
    ON f.id = rs.id_fecha
WHERE UPPER(tis.nombre) = UPPER('Desnutrición aguda')
GROUP BY
    d.nombre,
    m.nombre,
    f.anio
ORDER BY
    f.anio,
    d.nombre,
    m.nombre;

--20. Comparativa de casos de retardo en el desarrollo entre grupos etarios y sexo por departamento.
SELECT
    d.nombre AS departamento,
    ge.nombre AS grupo_etario,
    s.nombre AS sexo,
    SUM(rs.cantidad) AS total_casos
FROM registro_salud rs
JOIN tipo_indicador_salud tis
    ON tis.id = rs.id_tipo_indicador_salud
JOIN municipio m
    ON m.id = rs.id_municipio
JOIN departamento d
    ON d.id = m.id_departamento
LEFT JOIN grupo_etario ge
    ON ge.id = rs.id_grupo_etario
LEFT JOIN sexo s
    ON s.id = rs.id_sexo
WHERE UPPER(tis.nombre) = UPPER('Retardo en el desarrollo')
GROUP BY
    d.nombre,
    ge.nombre,
    s.nombre
ORDER BY
    d.nombre,
    ge.nombre,
    s.nombre;

--21. Departamentos con mayor incidencia de enfermedades crónicas según diagnóstico CIE-10.
SELECT FIRST 20
    d.nombre AS departamento,
    dc.codigo AS codigo_cie10,
    dc.nombre AS diagnostico_cie10,
    SUM(rs.cantidad) AS total_casos
FROM registro_salud rs
JOIN diagnostico_cie10 dc
    ON dc.id = rs.id_diagnostico
JOIN municipio m
    ON m.id = rs.id_municipio
JOIN departamento d
    ON d.id = m.id_departamento
WHERE rs.id_diagnostico IS NOT NULL
GROUP BY
    d.nombre,
    dc.codigo,
    dc.nombre
ORDER BY
    total_casos DESC,
    d.nombre,
    dc.codigo;

--22. Evolución anual de casos de dengue y dengue grave por departamento del 2012 al 2024.
SELECT
    f.anio AS anio,
    d.nombre AS departamento,
    e.nombre AS enfermedad,
    SUM(rs.cantidad) AS total_casos
FROM registro_salud rs
JOIN enfermedad e
    ON e.id = rs.id_enfermedad
JOIN fecha f
    ON f.id = rs.id_fecha
JOIN municipio m
    ON m.id = rs.id_municipio
JOIN departamento d
    ON d.id = m.id_departamento
WHERE f.anio BETWEEN 2012 AND 2024
  AND (
        UPPER(e.nombre) = UPPER('Dengue')
        OR UPPER(e.nombre) = UPPER('Dengue grave')
      )
GROUP BY
    f.anio,
    d.nombre,
    e.nombre
ORDER BY
    f.anio,
    d.nombre,
    e.nombre;

--23. Distribución de casos de malaria por grupo etario y sexo a nivel municipal.
SELECT
    m.nombre AS municipio,
    ge.nombre AS grupo_etario,
    s.nombre AS sexo,
    SUM(rs.cantidad) AS total_casos
FROM registro_salud rs
JOIN enfermedad e
    ON e.id = rs.id_enfermedad
JOIN municipio m
    ON m.id = rs.id_municipio
LEFT JOIN grupo_etario ge
    ON ge.id = rs.id_grupo_etario
LEFT JOIN sexo s
    ON s.id = rs.id_sexo
WHERE UPPER(e.nombre) = UPPER('Malaria')
GROUP BY
    m.nombre,
    ge.nombre,
    s.nombre
ORDER BY
    m.nombre,
    ge.nombre,
    s.nombre;

--24. Relación entre casos de desnutrición aguda infantil y registros de violencia intrafamiliar por departamento.
SELECT
    d.nombre AS departamento,
    COALESCE(ds.total_desnutricion_infantil, 0) AS total_desnutricion_infantil,
    COALESCE(vif.total_violencia_intrafamiliar, 0) AS total_violencia_intrafamiliar
FROM departamento d
LEFT JOIN (
    SELECT
        dep.id AS id_departamento,
        SUM(rs.cantidad) AS total_desnutricion_infantil
    FROM registro_salud rs
    JOIN tipo_indicador_salud tis
        ON tis.id = rs.id_tipo_indicador_salud
    JOIN grupo_etario ge
        ON ge.id = rs.id_grupo_etario
    JOIN municipio m
        ON m.id = rs.id_municipio
    JOIN departamento dep
        ON dep.id = m.id_departamento
    WHERE UPPER(tis.nombre) = UPPER('Desnutrición aguda')
      AND UPPER(ge.nombre) IN ('<1M', '1M-2M', '2M-1A', '1A-4A')
    GROUP BY
        dep.id
) ds
    ON ds.id_departamento = d.id
LEFT JOIN (
    SELECT
        dep.id AS id_departamento,
        COUNT(*) AS total_violencia_intrafamiliar
    FROM violencia_intrafamiliar vi
    JOIN hecho_delictivo hecho
        ON hecho.id = vi.id_hecho_delictivo
    JOIN municipio m
        ON m.id = hecho.id_municipio
    JOIN departamento dep
        ON dep.id = m.id_departamento
    GROUP BY
        dep.id
) vif
    ON vif.id_departamento = d.id
ORDER BY
    total_desnutricion_infantil DESC,
    total_violencia_intrafamiliar DESC,
    d.nombre;

--25. Top 5 municipios con mayor cantidad de casos de Chagas, Zika y Chikungunya combinados.
SELECT FIRST 5
    m.nombre AS municipio,
    d.nombre AS departamento,
    SUM(rs.cantidad) AS total_casos
FROM registro_salud rs
JOIN enfermedad e
    ON e.id = rs.id_enfermedad
JOIN municipio m
    ON m.id = rs.id_municipio
JOIN departamento d
    ON d.id = m.id_departamento
WHERE UPPER(e.nombre) IN (
    UPPER('Chagas'),
    UPPER('Zika'),
    UPPER('Chikungunya')
)
GROUP BY
    m.nombre,
    d.nombre
ORDER BY
    total_casos DESC,
    d.nombre,
    m.nombre;

--26. Comparativa entre enfermedades transmitidas por vectores y nivel de urbanización por departamento.
SELECT
    d.nombre AS departamento,
    e.nombre AS enfermedad_vectorial,
    SUM(rs.cantidad) AS total_casos
FROM registro_salud rs
JOIN tipo_indicador_salud tis
    ON tis.id = rs.id_tipo_indicador_salud
JOIN enfermedad e
    ON e.id = rs.id_enfermedad
JOIN municipio m
    ON m.id = rs.id_municipio
JOIN departamento d
    ON d.id = m.id_departamento
WHERE UPPER(tis.nombre) = UPPER('Enfermedades transmitidas por vectores')
GROUP BY
    d.nombre,
    e.nombre
ORDER BY
    d.nombre,
    total_casos DESC,
    e.nombre;

--27. Tasa de morbilidad materna infantil por año y departamento.
SELECT
    f.anio AS anio,
    d.nombre AS departamento,
    SUM(rs.cantidad) AS total_casos
FROM registro_salud rs
JOIN tipo_indicador_salud tis
    ON tis.id = rs.id_tipo_indicador_salud
JOIN fecha f
    ON f.id = rs.id_fecha
JOIN municipio m
    ON m.id = rs.id_municipio
JOIN departamento d
    ON d.id = m.id_departamento
WHERE UPPER(tis.nombre) = UPPER('Embarazos adolescentes')
GROUP BY
    f.anio,
    d.nombre
ORDER BY
    f.anio,
    total_casos DESC,
    d.nombre;

--28. Correlación entre casos de desnutrición y ocurrencia de hechos delictivos por municipio.
SELECT
    m.nombre AS municipio,
    d.nombre AS departamento,
    COALESCE(ds.total_desnutricion, 0) AS total_desnutricion,
    COALESCE(hecho.total_hechos_delictivos, 0) AS total_hechos_delictivos
FROM municipio m
JOIN departamento d
    ON d.id = m.id_departamento
LEFT JOIN (
    SELECT
        rs.id_municipio,
        SUM(rs.cantidad) AS total_desnutricion
    FROM registro_salud rs
    JOIN tipo_indicador_salud tis
        ON tis.id = rs.id_tipo_indicador_salud
    WHERE UPPER(tis.nombre) LIKE '%DESNUT%'
    GROUP BY
        rs.id_municipio
) ds
    ON ds.id_municipio = m.id
LEFT JOIN (
    SELECT
        hecho.id_municipio,
        COUNT(*) AS total_hechos_delictivos
    FROM hecho_delictivo hecho
    GROUP BY
        hecho.id_municipio
) hecho
    ON hecho.id_municipio = m.id
ORDER BY
    total_desnutricion DESC,
    total_hechos_delictivos DESC,
    d.nombre,
    m.nombre;

--29. Comparativa de casos de violencia estructural por departamento y tipo de discriminación
SELECT
    d.nombre AS departamento,
    td.nombre AS tipo_discriminacion,
    COUNT(*) AS total_casos
FROM caso_discriminacion cd
JOIN departamento d
    ON d.id = cd.id_departamento
JOIN tipo_discriminacion td
    ON td.id = cd.id_tipo_discriminacion
GROUP BY
    d.nombre,
    td.nombre
ORDER BY
    d.nombre,
    total_casos DESC,
    td.nombre;

--30. Frecuencia de exhumaciones por año y departamento.
SELECT
    f.anio,
    d.nombre AS departamento,
    COUNT(*) AS total_exhumaciones
FROM exhumacion e
JOIN fecha f
    ON f.id = e.id_fecha
JOIN departamento d
    ON d.id = e.id_departamento
GROUP BY
    f.anio,
    d.nombre
ORDER BY
    f.anio,
    total_exhumaciones DESC,
    d.nombre;