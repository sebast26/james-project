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

package org.apache.james.sieve.jpa;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceException;

import org.apache.commons.io.IOUtils;
import org.apache.james.core.User;
import org.apache.james.core.quota.QuotaSize;
import org.apache.james.sieve.jpa.model.JPASieveQuota;
import org.apache.james.sieve.jpa.model.JPASieveScript;
import org.apache.james.sieverepository.api.ScriptContent;
import org.apache.james.sieverepository.api.ScriptName;
import org.apache.james.sieverepository.api.ScriptSummary;
import org.apache.james.sieverepository.api.SieveRepository;
import org.apache.james.sieverepository.api.exception.DuplicateException;
import org.apache.james.sieverepository.api.exception.IsActiveException;
import org.apache.james.sieverepository.api.exception.QuotaExceededException;
import org.apache.james.sieverepository.api.exception.QuotaNotFoundException;
import org.apache.james.sieverepository.api.exception.ScriptNotFoundException;
import org.apache.james.sieverepository.api.exception.SieveRepositoryException;
import org.apache.james.sieverepository.api.exception.StorageException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JPASieveRepository implements SieveRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(JPASieveRepository.class);

    private static final String DEFAULT_SIEVE_QUOTA_USERNAME = "default.quota";

    private final EntityManagerFactory entityManagerFactory;

    @Inject
    public JPASieveRepository(EntityManagerFactory entityManagerFactory) {
        this.entityManagerFactory = entityManagerFactory;
    }

    @Override
    public void haveSpace(final User user, final ScriptName name, final long size) throws QuotaExceededException, StorageException {
        long usedSpace = findAllSieveScriptsForUser(user).stream()
                .filter(sieveScript -> !sieveScript.getScriptName().equals(name.getValue()))
                .mapToLong(JPASieveScript::getScriptSize)
                .sum();

        QuotaSize quota = limitToUse(user);
        if (overQuotaAfterModification(usedSpace, size, quota)) {
            throw new QuotaExceededException();
        }
    }

    private QuotaSize limitToUse(User user) throws StorageException {
        Optional<JPASieveQuota> userQuota = findQuotaForUser(user.asString());
        if (userQuota.isPresent()) {
            return QuotaSize.size(userQuota.map(JPASieveQuota::getSize));
        }

        Optional<JPASieveQuota> defaultQuota = findQuotaForUser(DEFAULT_SIEVE_QUOTA_USERNAME);
        if (defaultQuota.isPresent()) {
            return QuotaSize.size(defaultQuota.map(JPASieveQuota::getSize));
        } else {
            return QuotaSize.unlimited();
        }
    }

    private boolean overQuotaAfterModification(final long usedSpace, final long size, final QuotaSize quota) {
        return QuotaSize.size(usedSpace).add(size).isGreaterThan(quota);
    }

    @Override
    public void putScript(final User user, final ScriptName name, final ScriptContent content) throws StorageException, QuotaExceededException {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        EntityTransaction transaction = entityManager.getTransaction();
        try {
            transaction.begin();

            haveSpace(user, name, content.length());
            JPASieveScript jpaSieveScript = JPASieveScript.builder(user.asString(), name.getValue()).scriptContent(content).build();
            entityManager.merge(jpaSieveScript);

            transaction.commit();
        } catch (SieveRepositoryException e) {
            rollbackTransactionIfActive(transaction);
            throw e;
        } catch (PersistenceException e) {
            LOGGER.debug("Unable to put script for user " + user.asString(), e);
            rollbackTransactionIfActive(transaction);
            throw new StorageException("Unable to put script for user " + user.asString(), e);
        } finally {
            entityManager.close();
        }
    }

    private void rollbackTransactionIfActive(final EntityTransaction transaction) {
        if (transaction.isActive()) {
            transaction.rollback();
        }
    }

    @Override
    public List<ScriptSummary> listScripts(final User user) throws StorageException {
        return findAllSieveScriptsForUser(user).stream()
                .map(sieveScript -> new ScriptSummary(new ScriptName(sieveScript.getScriptName()), sieveScript.isActive()))
                .collect(Collectors.toList());
    }

    private List<JPASieveScript> findAllSieveScriptsForUser(final User user) throws StorageException {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        try {
            List<JPASieveScript> sieveScripts = entityManager.createNamedQuery("findAllByUsername", JPASieveScript.class)
                    .setParameter("username", user.asString()).getResultList();
            return sieveScripts != null ? sieveScripts : new ArrayList<>();
        } catch (PersistenceException e) {
            LOGGER.debug("Unable to list scripts for user " + user.asString(), e);
            throw new StorageException("Unable to list scripts for user " + user.asString(), e);
        } finally {
            entityManager.close();
        }
    }

    @Override
    public ZonedDateTime getActivationDateForActiveScript(final User user) throws StorageException, ScriptNotFoundException {
        Optional<JPASieveScript> script = findActiveSieveScript(user);
        JPASieveScript activeSieveScript = script.orElseThrow(() -> new ScriptNotFoundException("Unable to find active script for user " + user.asString()));
        return activeSieveScript.getActivationDateTime().toZonedDateTime();
    }

    @Override
    public InputStream getActive(final User user) throws ScriptNotFoundException, StorageException {
        Optional<JPASieveScript> script = findActiveSieveScript(user);
        JPASieveScript activeSieveScript = script.orElseThrow(() -> new ScriptNotFoundException("Unable to find active script for user " + user.asString()));
        return IOUtils.toInputStream(activeSieveScript.getScriptContent(), StandardCharsets.UTF_8);
    }

    private Optional<JPASieveScript> findActiveSieveScript(final User user) throws StorageException {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        try {
            return findActiveSieveScript(user, entityManager);
        } catch (PersistenceException e) {
            LOGGER.debug("Unable to find active script for user " + user.asString(), e);
            throw new StorageException("Unable to find active script for user " + user.asString(), e);
        } finally {
            entityManager.close();
        }
    }

    private Optional<JPASieveScript> findActiveSieveScript(final User user, final EntityManager entityManager) throws StorageException {
        try {
            JPASieveScript activeSieveScript = entityManager.createNamedQuery("findActiveByUsername", JPASieveScript.class)
                    .setParameter("username", user.asString()).getSingleResult();
            return Optional.ofNullable(activeSieveScript);
        } catch (NoResultException e) {
            LOGGER.debug("Unable to find active script for user " + user.asString(), e);
            return Optional.empty();
        }
    }

    @Override
    public void setActive(final User user, final ScriptName name) throws ScriptNotFoundException, StorageException {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        EntityTransaction transaction = entityManager.getTransaction();

        try {
            transaction.begin();

            if (SieveRepository.NO_SCRIPT_NAME.equals(name)) {
                switchOffActiveScript(user, name, entityManager);
            } else {
                setActiveScript(user, name, entityManager);
            }

            transaction.commit();
        } catch (SieveRepositoryException e) {
            rollbackTransactionIfActive(transaction);
            throw e;
        } catch (PersistenceException e) {
            LOGGER.debug("Unable to set active script " + name.getValue() + " for user " + user.asString(), e);
            rollbackTransactionIfActive(transaction);
            throw new StorageException("Unable to set active script " + name.getValue() + " for user " + user.asString(), e);
        } finally {
            entityManager.close();
        }
    }

    private void switchOffActiveScript(final User user, final ScriptName name, final EntityManager entityManager) throws StorageException {
        Optional<JPASieveScript> activeSieveScript = findActiveSieveScript(user, entityManager);
        activeSieveScript.ifPresent(JPASieveScript::deActivate);
    }

    private void setActiveScript(final User user, final ScriptName name, final EntityManager entityManager) throws StorageException, ScriptNotFoundException {
        JPASieveScript sieveScript = findSieveScript(user, name, entityManager)
                .orElseThrow(() -> new ScriptNotFoundException("Unable to find script " + name.getValue() + " for user " + user.asString()));
        // TODO: optioanl
        JPASieveScript activeSieveScript = findActiveSieveScript(user, entityManager).orElse(null);
        sieveScript.activate();
        if (activeSieveScript != null) {
            activeSieveScript.deActivate();
        }
    }

    @Override
    public InputStream getScript(final User user, final ScriptName name) throws ScriptNotFoundException, StorageException {
        Optional<JPASieveScript> script = findSieveScript(user, name);
        JPASieveScript sieveScript = script.orElseThrow(() -> new ScriptNotFoundException("Unable to find script " + name.getValue() + " for user " + user.asString()));
        return IOUtils.toInputStream(sieveScript.getScriptContent(), StandardCharsets.UTF_8);
    }

    private Optional<JPASieveScript> findSieveScript(final User user, final ScriptName scriptName) throws StorageException {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        try {
            return findSieveScript(user, scriptName, entityManager);
        } catch (PersistenceException e) {
            LOGGER.debug("Unable to find script " + scriptName.getValue() + " for user " + user.asString(), e);
            throw new StorageException("Unable to find script " + scriptName.getValue() + " for user " + user.asString(), e);
        } finally {
            entityManager.close();
        }
    }

    private Optional<JPASieveScript> findSieveScript(final User user, final ScriptName scriptName, final EntityManager entityManager) {
        try {
            JPASieveScript sieveScript = entityManager.createNamedQuery("findSieveScript", JPASieveScript.class)
                    .setParameter("username", user.asString())
                    .setParameter("scriptName", scriptName.getValue()).getSingleResult();
            return Optional.ofNullable(sieveScript);
        } catch (NoResultException e) {
            LOGGER.debug("Unable to find script " + scriptName.getValue() + " for user " + user.asString(), e);
            return Optional.empty();
        }
    }

    @Override
    public void deleteScript(final User user, final ScriptName name) throws ScriptNotFoundException, IsActiveException, StorageException {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        EntityTransaction transaction = entityManager.getTransaction();

        try {
            transaction.begin();
            JPASieveScript sieveScript = findSieveScript(user, name, entityManager)
                    .orElseThrow(() -> new ScriptNotFoundException("Unable to find script " + name.getValue() + " for user " + user.asString()));
            if (sieveScript.isActive()) {
                LOGGER.debug("Unable to delete active script " + name.getValue() + " for user " + user.asString());
                throw new IsActiveException("Unable to delete active script " + name.getValue() + " for user " + user.asString());
            }
            entityManager.remove(sieveScript);
            transaction.commit();
        } catch (SieveRepositoryException e) {
            rollbackTransactionIfActive(transaction);
            throw e;
        } catch (PersistenceException e) {
            LOGGER.debug("Unable to delete script " + name.getValue() + " for user " + user.asString(), e);
            rollbackTransactionIfActive(transaction);
            throw new StorageException("Unable to delete script " + name.getValue() + " for user " + user.asString());
        } finally {
            entityManager.close();
        }
    }

    @Override
    public void renameScript(final User user, final ScriptName oldName, final ScriptName newName) throws ScriptNotFoundException, DuplicateException, StorageException {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        EntityTransaction transaction = entityManager.getTransaction();

        try {
            transaction.begin();

            final Optional<JPASieveScript> sieveScript = findSieveScript(user, oldName, entityManager);
            if (!sieveScript.isPresent()) {
                rollbackTransactionIfActive(transaction);
                throw new ScriptNotFoundException("Unable to find script " + oldName.getValue() + " for user " + user.asString());
            }

            Optional<JPASieveScript> duplicatedSieveScript = findSieveScript(user, newName, entityManager);
            if (duplicatedSieveScript.isPresent()) {
                rollbackTransactionIfActive(transaction);
                throw new DuplicateException("Unable to rename script. Duplicate found " + newName.getValue() + " for user " + user.asString());
            }

            // TODO: change mapping?
            JPASieveScript oldSieveScript = sieveScript.get();
            entityManager.remove(oldSieveScript);
            JPASieveScript newSieveScript = JPASieveScript.builder(user.asString(), newName.getValue())
                    .scriptContent(oldSieveScript.getScriptContent())
                    .isActive(oldSieveScript.isActive())
                    .build();
            entityManager.merge(newSieveScript);

            transaction.commit();
        } catch (PersistenceException e) {
            LOGGER.debug("Unable to rename script " + oldName.getValue() + " for user " + user.asString(), e);
            rollbackTransactionIfActive(transaction);
            throw new StorageException("Unable to rename script " + oldName.getValue() + " for user " + user.asString());
        } finally {
            entityManager.close();
        }
    }

    @Override
    public boolean hasDefaultQuota() throws StorageException {
        Optional<JPASieveQuota> defaultQuota = findQuotaForUser(DEFAULT_SIEVE_QUOTA_USERNAME);
        return defaultQuota.isPresent();
    }

    @Override
    public QuotaSize getDefaultQuota() throws QuotaNotFoundException, StorageException {
        JPASieveQuota jpaSieveQuota = findQuotaForUser(DEFAULT_SIEVE_QUOTA_USERNAME)
                .orElseThrow(() -> new QuotaNotFoundException("Unable to find quota for default user"));
        return QuotaSize.size(jpaSieveQuota.getSize());
    }

    @Override
    public void setDefaultQuota(final QuotaSize quota) throws StorageException {
        setQuotaForUser(DEFAULT_SIEVE_QUOTA_USERNAME, quota);
    }

    @Override
    public void removeQuota() throws QuotaNotFoundException, StorageException {
        removeQuotaForUser(DEFAULT_SIEVE_QUOTA_USERNAME);
    }

    @Override
    public boolean hasQuota(final User user) throws StorageException {
        Optional<JPASieveQuota> quotaForUser = findQuotaForUser(user.asString());
        return quotaForUser.isPresent();
    }

    @Override
    public QuotaSize getQuota(final User user) throws QuotaNotFoundException, StorageException {
        JPASieveQuota jpaSieveQuota = findQuotaForUser(user.asString())
                .orElseThrow(() -> new QuotaNotFoundException("Unable to find quota for user " + user.asString()));
        return QuotaSize.size(jpaSieveQuota.getSize());
    }

    @Override
    public void setQuota(final User user, final QuotaSize quota) throws StorageException {
        setQuotaForUser(user.asString(), quota);
    }

    @Override
    public void removeQuota(final User user) throws QuotaNotFoundException, StorageException {
        removeQuotaForUser(user.asString());
    }

    private Optional<JPASieveQuota> findQuotaForUser(String username) throws StorageException {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        try {
            return findQuotaForUser(username, entityManager);
        } catch (PersistenceException e) {
            LOGGER.debug("Unable to find quota for user " + username, e);
            throw new StorageException("Unable to find quota for user " + username, e);
        } finally {
            entityManager.close();
        }
    }

    private Optional<JPASieveQuota> findQuotaForUser(final String username, final EntityManager entityManager) {
        try {
            JPASieveQuota sieveQuota = entityManager.createNamedQuery("findByUsername", JPASieveQuota.class)
                    .setParameter("username", username).getSingleResult();
            return Optional.of(sieveQuota);
        } catch (NoResultException e) {
            LOGGER.debug("Unable to find quota for user " + username);
            return Optional.empty();
        }
    }

    private void setQuotaForUser(String username, QuotaSize quota) throws StorageException {
        runAndThrowOnError(entityManager -> {
            JPASieveQuota sieveQuota = new JPASieveQuota(username, quota.asLong());
            entityManager.merge(sieveQuota);
        }, pe -> new StorageException("Unable to set quota for user " + username, pe));
    }

    private void removeQuotaForUser(String username) throws StorageException {
        runAndThrowOnError(entityManager -> {
            Optional<JPASieveQuota> quotaForUser = findQuotaForUser(username, entityManager);
            quotaForUser.ifPresent(entityManager::remove);
        }, pe -> new StorageException("Unable to remove quota for user " + username, pe));
    }

    private <E extends Throwable> void runAndThrowOnError(final Consumer<EntityManager> runner, final Function<PersistenceException, E> exceptionTranslator) throws E {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        EntityTransaction transaction = entityManager.getTransaction();
        try {
            transaction.begin();
            runner.accept(entityManager);
            transaction.commit();
        } catch (PersistenceException e) {
            LOGGER.warn("Could not execute transaction", e);
            rollbackTransactionIfActive(transaction);
            throw exceptionTranslator.apply(e);
        } finally {
            entityManager.close();
        }
    }
}
