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

import java.time.OffsetDateTime;
import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

import org.apache.james.sieverepository.api.ScriptContent;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

@Entity(name = "JamesSieveScript")
@Table(name = "JAMES_SIEVE_SCRIPT")
@NamedQueries({
        @NamedQuery(name = "findAllByUsername", query = "SELECT sieveScript FROM JamesSieveScript sieveScript WHERE sieveScript.id.username=:username"),
        @NamedQuery(name = "findActiveByUsername", query = "SELECT sieveScript FROM JamesSieveScript sieveScript WHERE sieveScript.id.username=:username AND sieveScript.isActive=true"),
        @NamedQuery(name = "findSieveScript", query = "SELECT sieveScript FROM JamesSieveScript sieveScript WHERE sieveScript.id.username=:username AND sieveScript.id.scriptName=:scriptName")
})
public class JPASieveScript {

    public static Builder builder(String username, String scriptName) {
        return new Builder(username, scriptName);
    }

    public static class Builder {

        private final String username;
        private final String scriptName;
        private String scriptContent;
        private long scriptSize;
        private boolean isActive;
        private OffsetDateTime activationDateTime;

        public Builder(String username, String scriptName) {
            this.username = username;
            this.scriptName = scriptName;
        }

        public Builder scriptContent(String scriptContent) {
            Preconditions.checkNotNull(scriptContent);
            this.scriptContent = scriptContent;
            this.scriptSize = scriptContent.length();
            return this;
        }

        public Builder scriptContent(ScriptContent scriptContent) {
            Preconditions.checkNotNull(scriptContent);
            this.scriptContent = scriptContent.getValue();
            this.scriptSize = scriptContent.length();
            return this;
        }

        public Builder isActive(boolean isActive) {
            this.isActive = isActive;
            this.activationDateTime = OffsetDateTime.now();
            return this;
        }

        public JPASieveScript build() {
            return new JPASieveScript(this);
        }
    }

    @EmbeddedId
    private JPASieveScriptId id;

    @Column(name = "SCRIPT_CONTENT", nullable = false, length = 1024)
    private String scriptContent;

    @Column(name = "SCRIPT_SIZE", nullable = false)
    private long scriptSize;

    @Column(name = "IS_ACTIVE", nullable = false)
    private boolean isActive;

    @Column(name = "ACTIVATION_DATE_TIME")
    private OffsetDateTime activationDateTime;

    protected JPASieveScript() {
    }

    private JPASieveScript(Builder builder) {
        this.id = new JPASieveScriptId(builder.username, builder.scriptName);
        this.scriptContent = builder.scriptContent;
        this.scriptSize = builder.scriptSize;
        this.isActive = builder.isActive;
        this.activationDateTime = builder.activationDateTime;
    }

    public String getUsername() {
        return id.getUsername();
    }

    public String getScriptName() {
        return id.getScriptName();
    }

    public String getScriptContent() {
        return scriptContent;
    }

    public long getScriptSize() {
        return scriptSize;
    }

    public boolean isActive() {
        return isActive;
    }

    public OffsetDateTime getActivationDateTime() {
        return activationDateTime;
    }

    public void activate() {
        this.isActive = true;
        this.activationDateTime = OffsetDateTime.now();
    }

    public void deActivate() {
        this.isActive = false;
        this.activationDateTime = null;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final JPASieveScript that = (JPASieveScript) o;
        return scriptSize == that.scriptSize &&
                isActive == that.isActive &&
                Objects.equals(id, that.id) &&
                Objects.equals(scriptContent, that.scriptContent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, scriptContent, scriptSize, isActive);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("username", id.getUsername())
                .add("scriptName", id.getScriptName())
                .add("isActive", isActive)
                .toString();
    }
}
