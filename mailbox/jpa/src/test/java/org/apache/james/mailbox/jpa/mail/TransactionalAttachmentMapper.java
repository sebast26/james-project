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
package org.apache.james.mailbox.jpa.mail;

import java.util.Collection;
import java.util.List;

import org.apache.james.mailbox.exception.AttachmentNotFoundException;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.model.Attachment;
import org.apache.james.mailbox.model.AttachmentId;
import org.apache.james.mailbox.model.MessageId;
import org.apache.james.mailbox.store.mail.AttachmentMapper;
import org.apache.james.mailbox.store.mail.model.Username;
import org.apache.james.mailbox.store.transaction.Mapper;

import com.google.common.base.Throwables;

public class TransactionalAttachmentMapper implements AttachmentMapper {

    private final JPAAttachmentMapper wrapped;

    public TransactionalAttachmentMapper(JPAAttachmentMapper wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public void endRequest() {
    }

    @Override
    public <T> T execute(Transaction<T> transaction) throws MailboxException {
        return transaction.run();
    }

    @Override
    public Attachment getAttachment(AttachmentId attachmentId) throws AttachmentNotFoundException {
        return wrapped.getAttachment(attachmentId);
    }

    @Override
    public List<Attachment> getAttachments(Collection<AttachmentId> attachmentIds) {
        return wrapped.getAttachments(attachmentIds);
    }

    @Override
    public void storeAttachmentForOwner(Attachment attachment, Username owner) throws MailboxException {
        try {
            wrapped.execute(Mapper.toTransaction(() -> wrapped.storeAttachmentForOwner(attachment, owner)));
        } catch (MailboxException e) {
            Throwables.propagate(e);
        }
    }

    @Override
    public void storeAttachmentsForMessage(Collection<Attachment> attachments, MessageId ownerMessageId) throws MailboxException {
        try {
            wrapped.execute(Mapper.toTransaction(() -> wrapped.storeAttachmentsForMessage(attachments, ownerMessageId)));
        } catch (MailboxException e) {
            Throwables.propagate(e);
        }
    }

    @Override
    public Collection<MessageId> getRelatedMessageIds(AttachmentId attachmentId) throws MailboxException {
        return wrapped.getRelatedMessageIds(attachmentId);
    }

    @Override
    public Collection<Username> getOwners(AttachmentId attachmentId) throws MailboxException {
        return wrapped.getOwners(attachmentId);
    }
}
