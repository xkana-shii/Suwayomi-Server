package suwayomi.tachidesk.global.controller

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import io.javalin.http.HttpStatus
import io.javalin.http.UnauthorizedResponse
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import suwayomi.tachidesk.global.impl.About
import suwayomi.tachidesk.global.impl.AboutDataClass
import suwayomi.tachidesk.global.impl.AppUpdate
import suwayomi.tachidesk.global.impl.UpdateDataClass
import suwayomi.tachidesk.server.JavalinSetup.Attribute
import suwayomi.tachidesk.server.JavalinSetup.future
import suwayomi.tachidesk.server.JavalinSetup.getAttribute
import suwayomi.tachidesk.server.user.requireUser
import suwayomi.tachidesk.server.util.handler
import suwayomi.tachidesk.server.util.withOperation
import kotlin.system.exitProcess

/** Settings Page/Screen */
object SettingsController {
    /** returns some static info about the current app build */
    val about =
        handler(
            documentWith = {
                withOperation {
                    summary("About Suwayomi-Server")
                    description("Returns some static info about the current app build")
                }
            },
            behaviorOf = { ctx ->
                ctx.getAttribute(Attribute.TachideskUser).requireUser()
                ctx.json(About.getAbout())
            },
            withResults = {
                json<AboutDataClass>(HttpStatus.OK)
            },
        )

    /** check for app updates */
    val checkUpdate =
        handler(
            documentWith = {
                withOperation {
                    summary("Suwayomi-Server update check")
                    description("Check for app updates")
                }
            },
            behaviorOf = { ctx ->
                ctx.getAttribute(Attribute.TachideskUser).requireUser()
                ctx.future {
                    future { AppUpdate.checkUpdate() }
                        .thenApply { ctx.json(it) }
                }
            },
            withResults = {
                json<Array<UpdateDataClass>>(HttpStatus.OK)
            },
        )

    /** Shuts down the server */
    @OptIn(kotlinx.coroutines.DelicateCoroutinesApi::class)
    val shutdown =
        handler(
            documentWith = {
                withOperation {
                    summary("Shutdown Suwayomi-Server")
                    description("Shuts down the server process.")
                }
            },
            behaviorOf = { ctx ->
                val remoteAddr = ctx.req().remoteAddr
                if (remoteAddr != "127.0.0.1" && remoteAddr != "::1" && remoteAddr != "0:0:0:0:0:0:0:1") {
                    throw UnauthorizedResponse("Shutdown only allowed from localhost.")
                }
                ctx.result("Server is shutting down.")
                ctx.status(HttpStatus.OK)
                GlobalScope.launch {
                    delay(250)
                    exitProcess(0)
                }
            },
            withResults = {
                httpCode(HttpStatus.OK)
            },
        )
}
