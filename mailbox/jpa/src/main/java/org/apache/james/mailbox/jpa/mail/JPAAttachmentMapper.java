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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.persistence.EntityManagerFactory;
import javax.persistence.NoResultException;

import org.apache.james.mailbox.exception.AttachmentNotFoundException;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.jpa.JPATransactionalMapper;
import org.apache.james.mailbox.jpa.ids.JPAMessageId;
import org.apache.james.mailbox.jpa.mail.model.JPAAttachment;
import org.apache.james.mailbox.model.Attachment;
import org.apache.james.mailbox.model.AttachmentId;
import org.apache.james.mailbox.model.MessageId;
import org.apache.james.mailbox.store.mail.AttachmentMapper;
import org.apache.james.mailbox.store.mail.model.Username;
import org.apache.openjpa.persistence.PersistenceException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class JPAAttachmentMapper extends JPATransactionalMapper implements AttachmentMapper {

    private final MessageId.Factory messageIdFactory = new JPAMessageId.Factory();

    public JPAAttachmentMapper(EntityManagerFactory entityManagerFactory) {
        super(entityManagerFactory);
    }

    @Override
    public Attachment getAttachment(AttachmentId attachmentId) throws AttachmentNotFoundException {
        try {
            Preconditions.checkArgument(attachmentId != null);
            return getEntityManager().createNamedQuery("findAttachmentById", JPAAttachment.class)
                    .setParameter("idParam", attachmentId.getId())
                    .getSingleResult()
                    .toAttachment();
        } catch (NoResultException e) {
            throw new AttachmentNotFoundException(attachmentId.getId());
        }
    }

    @Override
    public List<Attachment> getAttachments(Collection<AttachmentId> attachmentIds) {
        try {
            Preconditions.checkArgument(attachmentIds != null);
            return ImmutableList.copyOf(Iterables.transform(attachmentIds,
                    input ->
                            getEntityManager().createNamedQuery("findAttachmentById", JPAAttachment.class)
                                    .setParameter("idParam", input.getId())
                                    .getSingleResult().toAttachment()));
        } catch (NoResultException e) {
            return ImmutableList.of();
        }
    }

    @Override
    public void storeAttachmentForOwner(Attachment attachment, Username owner) throws MailboxException {
        try {
            Preconditions.checkArgument(attachment != null);
            Preconditions.checkArgument(owner != null);
            Optional<JPAAttachment> storedAttachment = this.getJPAAttachmentIfExists(attachment.getAttachmentId());
            JPAAttachment jpaAttachment = storedAttachment.orElse(JPAAttachment.from(attachment));
            jpaAttachment.setOwner(owner.getValue());
            getEntityManager().merge(jpaAttachment);
        } catch (PersistenceException e) {
            throw new MailboxException("Store of attachment for owner " + owner.getValue() + " failed", e);
        }
    }

    @Override
    public void storeAttachmentsForMessage(Collection<Attachment> attachments, MessageId ownerMessageId) throws MailboxException {
        try {
            Preconditions.checkArgument(attachments != null);
            Preconditions.checkArgument(ownerMessageId != null);
            for (Attachment attachment : attachments) {
                Optional<JPAAttachment> storedAttachment = this.getJPAAttachmentIfExists(attachment.getAttachmentId());
                JPAAttachment jpaAttachment = storedAttachment.orElse(JPAAttachment.from(attachment));
                jpaAttachment.setMessageId(ownerMessageId.serialize());
                getEntityManager().merge(jpaAttachment);
            }
        } catch (PersistenceException e) {
            throw new MailboxException("Store of attachments for messageId " + ownerMessageId.serialize() + " failed", e);
        }
    }

    @Override
    public Collection<MessageId> getRelatedMessageIds(AttachmentId attachmentId) throws MailboxException {
        try {
            final Optional<List<String>> messageIds = Optional.ofNullable(getEntityManager().createNamedQuery("findAttachmentById", JPAAttachment.class)
                    .setParameter("idParam", attachmentId.getId())
                    .getSingleResult().getMessageIds());

            return ImmutableList.copyOf(messageIds.orElse(new ArrayList<>())
                    .stream()
                    .map(messageId -> messageIdFactory.fromString(messageId))
                    .collect(Collectors.toList()));
        } catch (NoResultException e) {
            return ImmutableList.of();
        }
    }

    @Override
    public Collection<Username> getOwners(AttachmentId attachmentId) throws MailboxException {
        try {
            final Optional<List<String>> owners = Optional.ofNullable(getEntityManager().createNamedQuery("findAttachmentById", JPAAttachment.class)
                    .setParameter("idParam", attachmentId.getId())
                    .getSingleResult().getOwners());

            return ImmutableList.copyOf(owners.orElse(new ArrayList<>())
                    .stream()
                    .map(owner -> Username.fromRawValue(owner))
                    .collect(Collectors.toList()));
        } catch (NoResultException e) {
            return ImmutableList.of();
        }
    }

    private Optional<JPAAttachment> getJPAAttachmentIfExists(AttachmentId attachmentId) {
        try {
            Preconditions.checkArgument(attachmentId != null);
            return Optional.of(getEntityManager().createNamedQuery("findAttachmentById", JPAAttachment.class)
                    .setParameter("idParam", attachmentId.getId())
                    .getSingleResult());
        } catch (NoResultException e) {
            return Optional.empty();
        }
    }
}
