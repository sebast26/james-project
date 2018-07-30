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

package org.apache.james.sieve.jpa.model;

import java.io.Serializable;
import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.Embeddable;

@Embeddable
public class JPASieveScriptId implements Serializable {
    @Column(name = "USER_NAME", nullable = false, length = 100)
    private String username;

    @Column(name = "SCRIPT_NAME", nullable = false, length = 255)
    private String scriptName;

    public JPASieveScriptId() {
    }

    public JPASieveScriptId(final String username, final String scriptName) {
        this.username = username;
        this.scriptName = scriptName;
    }

    public String getUsername() {
        return username;
    }

    public String getScriptName() {
        return scriptName;
    }

    public void setScriptName(final String newName) {
        this.scriptName = newName;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final JPASieveScriptId that = (JPASieveScriptId) o;
        return Objects.equals(username, that.username) &&
                Objects.equals(scriptName, that.scriptName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(username, scriptName);
    }
}
