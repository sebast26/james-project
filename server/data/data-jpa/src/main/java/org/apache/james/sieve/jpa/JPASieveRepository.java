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
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
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
import org.apache.james.sieverepository.api.exception.StorageException;
import org.apache.openjpa.persistence.InvalidStateException;

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
        try {
            return getQuota(user);
        } catch (QuotaNotFoundException e) {
            LOGGER.debug("User does not have quota - checking default quota");
        }

        try {
            return getDefaultQuota();
        } catch (QuotaNotFoundException e) {
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
            final JPASieveScript newSieveScript = entityManager.merge(jpaSieveScript);
            LOGGER.info("New sieve script: " + newSieveScript);
            transaction.commit();
        } catch (PersistenceException e) {
            LOGGER.debug("Failed to put script for user " + user.asString(), e);
            if (transaction.isActive()) {
                transaction.rollback();
            }
            throw new StorageException("Unable to put script for user " + user.asString(), e);
        } finally {
            if (transaction.isActive()) {
                transaction.rollback();
            }
            entityManager.close();
        }
    }

    @Override
    public List<ScriptSummary> listScripts(final User user) throws StorageException {
        List<JPASieveScript> allSieveScripts = findAllSieveScriptsForUser(user);
        LOGGER.info("AllSieveScripts for user " + user.asString() + " are " + allSieveScripts);
        return allSieveScripts.stream()
                .map(sieveScript -> new ScriptSummary(new ScriptName(sieveScript.getScriptName()), sieveScript.isActive()))
                .collect(Collectors.toList());
    }

    private List<JPASieveScript> findAllSieveScriptsForUser(final User user) throws StorageException {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        try {
            List<JPASieveScript> sieveScripts = entityManager.createNamedQuery("findAllByUsername", JPASieveScript.class)
                    .setParameter("username", user.asString()).getResultList();
            LOGGER.info("After findAllByUsername " + sieveScripts);
            return sieveScripts != null ? sieveScripts : new ArrayList<>();
        } catch (PersistenceException e) {
            LOGGER.debug("Failed to list scripts for user " + user.asString(), e);
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
        } catch (PersistenceException e) {
            LOGGER.debug("Unable to get active script for user " + user.asString(), e);
            throw new StorageException("Unable to get active script for user " + user.asString(), e);
        } catch (InvalidStateException e) {
            // TODO: remove
            LOGGER.error("InvalidStateException thrown ", e.getMessage(), e);
            LOGGER.info(ExceptionUtils.getStackTrace(e));
            throw new StorageException();
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
        } catch (PersistenceException e) {
            LOGGER.debug("Unable to set active script " + name.getValue() + " for user " + user.asString(), e);
            if (transaction.isActive()) {
                transaction.rollback();
            }
            throw new StorageException("Unable to set active script " + name.getValue() + " for user " + user.asString(), e);
        } finally {
            if (transaction.isActive()) {
                transaction.rollback();
            }
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
        } finally {
            entityManager.close();
        }
    }

    private Optional<JPASieveScript> findSieveScript(final User user, final ScriptName scriptName, final EntityManager entityManager) throws StorageException {
        try {
            JPASieveScript sieveScript = entityManager.createNamedQuery("findSieveScript", JPASieveScript.class)
                    .setParameter("username", user.asString())
                    .setParameter("scriptName", scriptName.getValue()).getSingleResult();
            return Optional.ofNullable(sieveScript);
        } catch (NoResultException e) {
            LOGGER.debug("Unable to find script " + scriptName.getValue() + " for user " + user.asString(), e);
            return Optional.empty();
        } catch (PersistenceException e) {
            LOGGER.debug("Unable to find script " + scriptName.getValue() + " for user " + user.asString(), e);
            throw new StorageException("Unable to find script " + scriptName.getValue() + " for user " + user.asString(), e);
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
        } catch (PersistenceException e) {
            LOGGER.debug("Unable to delete script " + name.getValue() + " for user " + user.asString(), e);
            if (transaction.isActive()) {
                transaction.rollback();
            }
            throw new StorageException("Unable to delete script " + name.getValue() + " for user " + user.asString());
        } finally {
            if (transaction.isActive()) {
                transaction.rollback();
            }
            entityManager.close();
        }
    }

    @Override
    public void renameScript(final User user, final ScriptName oldName, final ScriptName newName) throws ScriptNotFoundException, DuplicateException, StorageException {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        EntityTransaction transaction = entityManager.getTransaction();

        try {
            transaction.begin();

            JPASieveScript sieveScript = findSieveScript(user, oldName, entityManager)
                    .orElseThrow(() -> new ScriptNotFoundException("Unable to find script " + oldName.getValue() + " for user " + user.asString()));
            Optional<JPASieveScript> duplicatedSieveScript = findSieveScript(user, newName, entityManager);
            if (duplicatedSieveScript.isPresent()) {
                throw new DuplicateException("Unable to rename script. Duplicate found " + newName.getValue() + " for user " + user.asString());
            }

            // TODO: change mapping?
            entityManager.remove(sieveScript);
            JPASieveScript renamedSieveScript = JPASieveScript.builder(user.asString(), newName.getValue()).scriptContent(sieveScript.getScriptContent())
                    .isActive(sieveScript.isActive()).build();
            entityManager.merge(renamedSieveScript);

            transaction.commit();
        } catch (PersistenceException e) {
            LOGGER.debug("Unable to rename script " + oldName.getValue() + " for user " + user.asString(), e);
            if (transaction.isActive()) {
                transaction.rollback();
            }
            throw new StorageException("Unable to rename script " + oldName.getValue() + " for user " + user.asString());
        } finally {
            // TODO: refactor
            if (transaction.isActive()) {
                transaction.rollback();
            }
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
        } finally {
            entityManager.close();
        }
    }

    private Optional<JPASieveQuota> findQuotaForUser(final String username, final EntityManager entityManager) throws StorageException {
        try {
            JPASieveQuota sieveQuota = entityManager.createNamedQuery("findByUsername", JPASieveQuota.class)
                    .setParameter("username", username).getSingleResult();
            return Optional.of(sieveQuota);
        } catch (NoResultException e) {
            LOGGER.debug("Unable to find quota for user " + username);
            return Optional.empty();
        } catch (PersistenceException e) {
            LOGGER.debug("Unable to find quota for user " + username, e);
            throw new StorageException("Unable to find quota for user " + username, e);
        }
    }

    private void setQuotaForUser(String username, QuotaSize quota) throws StorageException {
        EntityManager entityManager = entityManagerFactory.createEntityManager();

        EntityTransaction transaction = entityManager.getTransaction();
        try {
            transaction.begin();

            JPASieveQuota sieveQuota = new JPASieveQuota(username, quota.asLong());
            entityManager.merge(sieveQuota);

            transaction.commit();
        } catch (PersistenceException e) {
            LOGGER.debug("Failed to set quota for user " + username, e);
            if (transaction.isActive()) {
                transaction.rollback();
            }
            throw new StorageException("Unable to set quota for user " + username, e);
        } finally {
            entityManager.close();
        }
    }

    private void removeQuotaForUser(String username) throws StorageException {
        EntityManager entityManager = entityManagerFactory.createEntityManager();

        EntityTransaction transaction = entityManager.getTransaction();
        try {
            transaction.begin();

            Optional<JPASieveQuota> quotaForUser = findQuotaForUser(username, entityManager);
            quotaForUser.ifPresent(entityManager::remove);

            transaction.commit();
        } catch (PersistenceException e) {
            LOGGER.debug("Failed to remove quota for user " + username, e);
            if (transaction.isActive()) {
                transaction.rollback();
            }
            throw new StorageException("Unable to remove quota for user " + username, e);
        } finally {
            // TODO? inside try/catch there can be StorageException thrown that is not handled on catch block
            if (transaction.isActive()) {
                transaction.rollback();
            }
            entityManager.close();
        }
    }
}
