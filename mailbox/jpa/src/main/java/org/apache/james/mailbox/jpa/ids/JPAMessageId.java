/****************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one   *
 * or more contributor license agreements.  See the NOTICE file *
 * distributed with this work for additional information        *
 * regarding copyright ownership.  The ASF licenses this file   *
 * to you under the Apache License, Version 2.0 (the            *
 * "License"); you may not use this file except in compliance   *
 * with the License.  You may obtain a copy of the License at   *
 *                                                              *
 *   http://www.apache.org/licenses/LICENSE-2.0                 *
 *                                                              *
 * Unless required by applicable law or agreed to in writing,   *
 * software distributed under the License is distributed on an  *
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
 * KIND, either express or implied.  See the License for the    *
 * specific language governing permissions and limitations      *
 * under the License.                                           *
 ****************************************************************/
package org.apache.james.mailbox.jpa.ids;

import java.util.Objects;
import java.util.UUID;

import org.apache.james.mailbox.model.MessageId;

import com.google.common.base.MoreObjects;

public class JPAMessageId implements MessageId {

    public static class Factory implements MessageId.Factory {
        @Override
        public JPAMessageId generate() {
            return of(UUID.randomUUID());
        }

        public JPAMessageId of(UUID uuid) {
            return new JPAMessageId(uuid);
        }

        @Override
        public MessageId fromString(String serialized) {
            return of(UUID.fromString(serialized));
        }
    }

    private final UUID uuid;

    private JPAMessageId(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public String serialize() {
        return uuid.toString();
    }

    public UUID get() {
        return uuid;
    }

    @Override
    public final boolean equals(Object o) {
        if (o instanceof JPAMessageId) {
            JPAMessageId other = (JPAMessageId) o;
            return Objects.equals(uuid, other.uuid);
        }
        return false;
    }

    @Override
    public final int hashCode() {
        return Objects.hash(uuid);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("uuid", uuid)
                .toString();
    }
}