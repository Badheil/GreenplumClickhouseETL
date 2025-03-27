# GreenplumClickhouseETL
This repository contains a project to automate the processes of uploading and processing data for analytics and building data storefronts.
Основные компоненты проекта:

Загрузка данных: Использование PXF и gpfdist для загрузки данных в Greenplum.
Обработка данных: Реализация партиционирования и дистрибуции данных в таблицах.
Функции: Создание функций для загрузки данных и расчета витрин данных.
Интеграция с ClickHouse: Загрузка данных в ClickHouse для быстрой аналитики.
Визуализация: Построение дашборда для визуализации данных.
Автоматизация: Полная автоматизация процессов с помощью Apache Airflow, включая DAG для управления загрузкой и обработкой данных.
Технологии:

Greenplum
ClickHouse
Apache Airflow
SuperSet
PXF, gpfdist
