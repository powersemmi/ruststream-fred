---
slug: /
sidebar_position: 0
description: "Руководства по сервисам на Redis и Valkey с ruststream-fred, брокером Redis для RustStream."
---

# Руководства

Крейт `ruststream-fred` реализует брокер Redis и Valkey для [RustStream](https://powersemmi.github.io/ruststream/latest/ru/).
Он построен на клиенте [fred](https://docs.rs/fred). Сервис читает потоки Redis через группы
потребителей, список как очередь заданий или каналы Pub/Sub. Сервер может быть одиночным, кластером
или набором под надзором Sentinel. Руководства проводят сервис на Redis от установки до промышленной
эксплуатации.
