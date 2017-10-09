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
package org.apache.james.mailbox.jpa.mail.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

import org.apache.james.mailbox.model.Attachment;
import org.apache.james.mailbox.model.AttachmentId;

import com.google.common.base.MoreObjects;

@Entity(name = "Attachment")
@Table(name = "JAMES_ATTACHMENT")
@NamedQueries({
        @NamedQuery(name = "findAttachmentById",
                query = "SELECT attachment FROM Attachment attachment WHERE attachment.attachmentId = :idParam")

})
public class JPAAttachment {

    @Id
    @Column(name = "ATTACHMENT_ID")
    private String attachmentId;

    @Lob
    @Column(name = "ATTACHMENT_BYTES", columnDefinition = "BLOB NOT NULL")
    private byte[] bytes;

    @Basic(optional = false)
    @Column(name = "ATTACHMENT_TYPE", nullable = false)
    private String type;

    @Column(name = "ATTACHMENT_OWNER")
    @ElementCollection(targetClass = String.class)
    private ArrayList<String> owners;

    @Column(name = "MESSAGE_ID")
    @ElementCollection(targetClass = String.class)
    private ArrayList<String> messageIds;

    public static JPAAttachment from(Attachment attachment) {
        return new JPAAttachment(attachment);
    }

    public JPAAttachment(Attachment attachment) {
        this.setAttachmentId(attachment.getAttachmentId().getId());
        this.type = attachment.getType();
        this.bytes = attachment.getBytes();
        this.owners = new ArrayList<>();
        this.messageIds = new ArrayList<>();
    }

    public JPAAttachment(String attachmentId, byte[] bytes, String type, ArrayList<String> owners, ArrayList<String> messageIds) {
        this.attachmentId = attachmentId;
        this.bytes = bytes;
        this.type = type;
        this.owners = owners;
        this.messageIds = messageIds;
    }

    public String getAttachmentId() {
        return attachmentId;
    }

    public void setAttachmentId(String attachmentId) {
        this.attachmentId = attachmentId;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<String> getOwners() {
        return owners;
    }

    public void setOwners(ArrayList<String> owners) {
        this.owners = owners;
    }

    public void setOwner(String owner) {
        this.owners.add(owner);
    }

    public List<String> getMessageIds() {
        return messageIds;
    }

    public void setMessageId(String messageId) {
        this.messageIds.add(messageId);
    }

    public void setMessageIds(ArrayList<String> messageIds) {
        this.messageIds = messageIds;
    }


    public Attachment toAttachment() {
        return Attachment.builder()
                .bytes(this.bytes)
                .attachmentId(AttachmentId.from(this.attachmentId))
                .type(this.type)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof JPAAttachment) {
            JPAAttachment other = (JPAAttachment) o;
            return Objects.equals(messageIds, other.messageIds)
                    && Arrays.equals(bytes, other.bytes)
                    && Objects.equals(attachmentId, other.attachmentId)
                    && Objects.equals(type, other.type)
                    && Objects.equals(owners, other.owners);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(attachmentId, bytes, type, owners, messageIds);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("attachmentId", attachmentId)
                .add("bytes", bytes)
                .add("type", type)
                .add("owners", owners)
                .add("messageIds", messageIds)
                .toString();
    }
}
