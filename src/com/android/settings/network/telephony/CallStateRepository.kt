/*
 * Copyright (C) 2024 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Changes from Qualcomm Technologies, Inc. are provided under the following license:
 * Copyright (c) Qualcomm Technologies, Inc. and/or its subsidiaries.
 * SPDX-License-Identifier: BSD-3-Clause-Clear
 */

package com.android.settings.network.telephony

import android.content.Context
import android.telecom.TelecomManager
import android.telephony.SubscriptionManager
import android.telephony.TelephonyCallback
import android.telephony.TelephonyManager
import android.util.Log
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.conflate
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.shareIn

@OptIn(ExperimentalCoroutinesApi::class)
class CallStateRepository(
    private val context: Context,
    private val subscriptionRepository: SubscriptionRepository = SubscriptionRepository(context),
) {

    /** Flow for call state of given [subId]. */
    fun callStateFlow(subId: Int): Flow<Int> = context.telephonyCallbackFlow(subId) {
        object : TelephonyCallback(), TelephonyCallback.CallStateListener {
            override fun onCallStateChanged(state: Int) {
                trySend(state)
            }
        }
    }

    /**
     * Flow for in call state.
     *
     * @return true if any subscription's call state is not idle.
     */
    fun isInCallFlow(): Flow<Boolean> = subscriptionRepository.activeSubscriptionIdListFlow()
        .flatMapLatest { subIds ->
            if (subIds.isEmpty()) {
                // No active subscriptions — still monitor the default phone so that
                // emergency calls placed without a valid SIM are detected.
                // Query once immediately via isInEmergencyCallFlow for the initial state,
                // then monitor ongoing changes via a shared callStateFlow to avoid
                // duplicate callback registration when multiple instances exist.
                Log.d(TAG, "isInCallFlow: no active subs, query emergency call state")
                merge(
                    isInEmergencyCallFlow(),
                    getSharedDefaultSubCallStateFlow()
                        .map { it != TelephonyManager.CALL_STATE_IDLE },
                )
            } else {
                combine(subIds.map(::callStateFlow)) { states ->
                    states.any { it != TelephonyManager.CALL_STATE_IDLE }
                }
            }
        }
        .distinctUntilChanged()
        .conflate()
        .onEach { Log.d(TAG, "isInCallFlow: $it") }
        .flowOn(Dispatchers.Default)

    fun isInEmergencyCallFlow(): Flow<Boolean> {
        val telecomManager = context.getSystemService(TelecomManager::class.java)
        return if (telecomManager == null)
            flowOf(false)
        else
            flowOf(telecomManager.isInEmergencyCall)
    }

    private fun getSharedDefaultSubCallStateFlow(): Flow<Int> =
        getOrCreateSharedDefaultSubCallStateFlow(context.applicationContext)

    private companion object {
        private const val TAG = "CallStateRepository"
        private val repositoryScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

        @Volatile
        private var sharedDefaultSubCallStateFlow: SharedFlow<Int>? = null

        @Synchronized
        private fun getOrCreateSharedDefaultSubCallStateFlow(context: Context): SharedFlow<Int> =
            sharedDefaultSubCallStateFlow ?: context.telephonyCallbackFlow(
                SubscriptionManager.DEFAULT_SUBSCRIPTION_ID,
            ) {
                object : TelephonyCallback(), TelephonyCallback.CallStateListener {
                    override fun onCallStateChanged(state: Int) {
                        trySend(state)
                    }
                }
            }.shareIn(repositoryScope, SharingStarted.WhileSubscribed(), replay = 1)
                .also { sharedDefaultSubCallStateFlow = it }
    }
}
