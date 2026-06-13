package com.eventlens.model;

public enum EventSource {
    IVS_STAGE,
    IVS_EVENTBRIDGE,
    IVS_CHAT,
    CUSTOM_WEBSOCKET,
    ANDROID_CLIENT,
    IOS_CLIENT,
    BACKEND_MODERATOR,
    LAMBDA_CONSUMER,
    SQS_DLQ_REPROCESSOR
}
