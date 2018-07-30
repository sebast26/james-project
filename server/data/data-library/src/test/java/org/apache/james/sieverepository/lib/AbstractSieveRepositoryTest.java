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
package org.apache.james.sieverepository.lib;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.apache.commons.io.IOUtils;
import org.apache.james.core.User;
import org.apache.james.core.quota.QuotaSize;
import org.apache.james.sieverepository.api.ScriptContent;
import org.apache.james.sieverepository.api.ScriptName;
import org.apache.james.sieverepository.api.ScriptSummary;
import org.apache.james.sieverepository.api.SieveRepository;
import org.apache.james.sieverepository.api.exception.DuplicateException;
import org.apache.james.sieverepository.api.exception.IsActiveException;
import org.apache.james.sieverepository.api.exception.QuotaExceededException;
import org.apache.james.sieverepository.api.exception.QuotaNotFoundException;
import org.apache.james.sieverepository.api.exception.ScriptNotFoundException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSieveRepositoryTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSieveRepositoryTest.class);

    protected static final User USER = User.fromUsername("test");
    protected static final ScriptName SCRIPT_NAME = new ScriptName("script");
    protected static final ScriptContent SCRIPT_CONTENT = new ScriptContent("Hello World");

    private static final ScriptName OTHER_SCRIPT_NAME = new ScriptName("other_script");
    private static final ScriptContent OTHER_SCRIPT_CONTENT = new ScriptContent("Other script content");
    private static final QuotaSize DEFAULT_QUOTA = QuotaSize.size(Long.MAX_VALUE - 1L);
    private static final QuotaSize USER_QUOTA = QuotaSize.size(Long.MAX_VALUE / 2);

    protected SieveRepository sieveRepository;

    public void setUp() throws Exception {
        sieveRepository = createSieveRepository();
    }

    @Test(expected = ScriptNotFoundException.class)
    public void getScriptShouldThrowIfUnableToFindScript() throws Exception {
        LOGGER.info("getScriptShouldThrowIfUnableToFindScript");
        sieveRepository.getScript(USER, SCRIPT_NAME);
    }

    @Test
    public void getScriptShouldReturnCorrectContent() throws Exception {
        LOGGER.info("getScriptShouldReturnCorrectContent");
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
        assertThat(getScriptContent(sieveRepository.getScript(USER, SCRIPT_NAME))).isEqualTo(SCRIPT_CONTENT);
    }

    @Test
    public void getActivationDateForActiveScriptShouldReturnNonNullAndNonZeroResult() throws Exception {
        LOGGER.info("getActivationDateForActiveScriptShouldReturnNonNullAndNonZeroResult");
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
        sieveRepository.setActive(USER, SCRIPT_NAME);
        assertThat(sieveRepository.getActivationDateForActiveScript(USER)).isNotNull();
        assertThat(sieveRepository.getActivationDateForActiveScript(USER)).isNotEqualTo(ZonedDateTime.ofInstant(Instant.ofEpochMilli(0L), ZoneOffset.UTC));
    }

    @Test(expected = ScriptNotFoundException.class)
    public void getActivationDateForActiveScriptShouldThrowOnMissingActiveScript() throws Exception {
        LOGGER.info("getActivationDateForActiveScriptShouldThrowOnMissingActiveScript");
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
        sieveRepository.getActivationDateForActiveScript(USER);
    }

    @Test
    public void haveSpaceShouldNotThrowWhenUserDoesNotHaveQuota() throws Exception {
        LOGGER.info("haveSpaceShouldNotThrowWhenUserDoesNotHaveQuota");
        sieveRepository.haveSpace(USER, SCRIPT_NAME, DEFAULT_QUOTA.asLong() + 1L);
    }

    @Test
    public void haveSpaceShouldNotThrowWhenQuotaIsNotReached() throws Exception {
        LOGGER.info("haveSpaceShouldNotThrowWhenQuotaIsNotReached");
        sieveRepository.setDefaultQuota(DEFAULT_QUOTA);
        sieveRepository.haveSpace(USER, SCRIPT_NAME, DEFAULT_QUOTA.asLong());
    }

    @Test(expected = QuotaExceededException.class)
    public void haveSpaceShouldThrowWhenQuotaIsExceed() throws Exception {
        LOGGER.info("haveSpaceShouldThrowWhenQuotaIsExceed");
        sieveRepository.setDefaultQuota(DEFAULT_QUOTA);
        sieveRepository.haveSpace(USER, SCRIPT_NAME, DEFAULT_QUOTA.asLong() + 1);
    }

    @Test
    public void haveSpaceShouldNotThrowWhenAttemptToReplaceOtherScript() throws Exception {
        LOGGER.info("haveSpaceShouldNotThrowWhenAttemptToReplaceOtherScript");
        sieveRepository.setQuota(USER, USER_QUOTA);
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
        sieveRepository.haveSpace(USER, SCRIPT_NAME, USER_QUOTA.asLong());
    }

    @Test(expected = QuotaExceededException.class)
    public void haveSpaceShouldThrowWhenAttemptToReplaceOtherScriptWithTooLargeScript() throws Exception {
        LOGGER.info("haveSpaceShouldThrowWhenAttemptToReplaceOtherScriptWithTooLargeScript");
        sieveRepository.setQuota(USER, USER_QUOTA);
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
        sieveRepository.haveSpace(USER, SCRIPT_NAME, USER_QUOTA.asLong() + 1);
    }

    @Test(expected = QuotaExceededException.class)
    public void haveSpaceShouldTakeAlreadyExistingScriptsIntoAccount() throws Exception {
        LOGGER.info("haveSpaceShouldTakeAlreadyExistingScriptsIntoAccount");
        sieveRepository.setQuota(USER, USER_QUOTA);
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
        sieveRepository.haveSpace(USER, OTHER_SCRIPT_NAME, USER_QUOTA.asLong() - 1);
    }

    @Test
    public void haveSpaceShouldNotThrowAfterActivatingAScript() throws Exception {
        LOGGER.info("haveSpaceShouldNotThrowAfterActivatingAScript");
        sieveRepository.setQuota(USER, USER_QUOTA);
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
        sieveRepository.setActive(USER, SCRIPT_NAME);
        sieveRepository.haveSpace(USER, SCRIPT_NAME, USER_QUOTA.asLong());
    }

    @Test
    public void listScriptsShouldThrowIfUserNotFound() throws Exception {
        LOGGER.info("listScriptsShouldThrowIfUserNotFound");
        assertThat(sieveRepository.listScripts(USER)).isEmpty();
    }

    @Test
    public void listScriptsShouldReturnEmptyListWhenThereIsNoScript() throws Exception {
        LOGGER.info("listScriptsShouldReturnEmptyListWhenThereIsNoScript");
        assertThat(sieveRepository.listScripts(USER)).isEmpty();
    }

    @Test
    public void putScriptShouldWork() throws Exception {
        LOGGER.info("putScriptShouldWork");
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
        assertThat(sieveRepository.listScripts(USER)).containsOnly(new ScriptSummary(SCRIPT_NAME, false));
    }

    @Test
    public void setActiveShouldWork() throws Exception {
        LOGGER.info("setActiveShouldWork");
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
        final InputStream script = sieveRepository.getScript(USER, SCRIPT_NAME);
        LOGGER.info("script " + script.toString());
        sieveRepository.setActive(USER, SCRIPT_NAME);
        assertThat(sieveRepository.listScripts(USER)).containsOnly(new ScriptSummary(SCRIPT_NAME, true));
    }

    @Test
    public void listScriptShouldCombineActiveAndPassiveScripts() throws Exception {
        LOGGER.info("listScriptShouldCombineActiveAndPassiveScripts");
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
        sieveRepository.setActive(USER, SCRIPT_NAME);
        sieveRepository.putScript(USER, OTHER_SCRIPT_NAME, OTHER_SCRIPT_CONTENT);
        assertThat(sieveRepository.listScripts(USER)).containsOnly(new ScriptSummary(SCRIPT_NAME, true), new ScriptSummary(OTHER_SCRIPT_NAME, false));
    }

    @Test(expected = QuotaExceededException.class)
    public void putScriptShouldThrowWhenScriptTooBig() throws Exception {
        LOGGER.info("putScriptShouldThrowWhenScriptTooBig");
        sieveRepository.setDefaultQuota(QuotaSize.size(SCRIPT_CONTENT.length() - 1));
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
    }

    @Test(expected = QuotaExceededException.class)
    public void putScriptShouldThrowWhenQuotaChangedInBetween() throws Exception {
        LOGGER.info("putScriptShouldThrowWhenQuotaChangedInBetween");
        sieveRepository.setDefaultQuota(QuotaSize.size(SCRIPT_CONTENT.length()));
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
        sieveRepository.setDefaultQuota(QuotaSize.size(SCRIPT_CONTENT.length() - 1));
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
    }

    @Test(expected = ScriptNotFoundException.class)
    public void setActiveScriptShouldThrowOnNonExistentScript() throws Exception {
        LOGGER.info("setActiveScriptShouldThrowOnNonExistentScript");
        sieveRepository.setActive(USER, SCRIPT_NAME);
    }

    @Test
    public void setActiveScriptShouldWork() throws Exception {
        LOGGER.info("setActiveScriptShouldWork");
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
        sieveRepository.setActive(USER, SCRIPT_NAME);
        assertThat(getScriptContent(sieveRepository.getActive(USER))).isEqualTo(SCRIPT_CONTENT);
    }

    @Test
    public void setActiveSwitchScriptShouldWork() throws Exception {
        LOGGER.info("setActiveSwitchScriptShouldWork");
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
        sieveRepository.setActive(USER, SCRIPT_NAME);
        sieveRepository.putScript(USER, OTHER_SCRIPT_NAME, OTHER_SCRIPT_CONTENT);
        sieveRepository.setActive(USER, OTHER_SCRIPT_NAME);
        assertThat(getScriptContent(sieveRepository.getActive(USER))).isEqualTo(OTHER_SCRIPT_CONTENT);
    }

    @Test(expected = ScriptNotFoundException.class)
    public void switchOffActiveScriptShouldWork() throws Exception {
        LOGGER.info("switchOffActiveScriptShouldWork");
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
        sieveRepository.setActive(USER, SCRIPT_NAME);
        sieveRepository.setActive(USER, SieveRepository.NO_SCRIPT_NAME);
        sieveRepository.getActive(USER);
    }

    @Test
    public void switchOffActiveScriptShouldNotThrow() throws Exception {
        LOGGER.info("switchOffActiveScriptShouldNotThrow");
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
        sieveRepository.setActive(USER, SCRIPT_NAME);
        sieveRepository.setActive(USER, SieveRepository.NO_SCRIPT_NAME);
    }

    @Test(expected = ScriptNotFoundException.class)
    public void getActiveShouldThrowWhenNoActiveScript() throws Exception {
        LOGGER.info("getActiveShouldThrowWhenNoActiveScript");
        sieveRepository.getActive(USER);
    }

    @Test(expected = ScriptNotFoundException.class)
    public void deleteActiveScriptShouldThrowIfScriptDoNotExist() throws Exception {
        LOGGER.info("deleteActiveScriptShouldThrowIfScriptDoNotExist");
        sieveRepository.deleteScript(USER, SCRIPT_NAME);
    }

    @Test(expected = IsActiveException.class)
    public void deleteActiveScriptShouldThrow() throws Exception {
        LOGGER.info("deleteActiveScriptShouldThrow");
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
        sieveRepository.setActive(USER, SCRIPT_NAME);
        sieveRepository.deleteScript(USER, SCRIPT_NAME);
    }

    @Test(expected = IsActiveException.class)
    public void deleteScriptShouldWork() throws Exception {
        LOGGER.info("deleteScriptShouldWork");
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
        sieveRepository.setActive(USER, SCRIPT_NAME);
        sieveRepository.deleteScript(USER, SCRIPT_NAME);
        sieveRepository.getScript(USER, SCRIPT_NAME);
    }

    @Test(expected = ScriptNotFoundException.class)
    public void renameScriptShouldThrowIfScriptNotFound() throws Exception {
        LOGGER.info("renameScriptShouldThrowIfScriptNotFound");
        sieveRepository.renameScript(USER, SCRIPT_NAME, OTHER_SCRIPT_NAME);
    }

    @Test
    public void renameScriptShouldWork() throws Exception {
        LOGGER.info("renameScriptShouldWork");
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
        sieveRepository.renameScript(USER, SCRIPT_NAME, OTHER_SCRIPT_NAME);
        assertThat(getScriptContent(sieveRepository.getScript(USER, OTHER_SCRIPT_NAME))).isEqualTo(SCRIPT_CONTENT);
    }

    @Test
    public void renameScriptShouldPropagateActiveScript() throws Exception {
        LOGGER.info("renameScriptShouldPropagateActiveScript");
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
        sieveRepository.setActive(USER, SCRIPT_NAME);
        sieveRepository.renameScript(USER, SCRIPT_NAME, OTHER_SCRIPT_NAME);
        assertThat(getScriptContent(sieveRepository.getActive(USER))).isEqualTo(SCRIPT_CONTENT);
    }

    @Test(expected = DuplicateException.class)
    public void renameScriptShouldNotOverwriteExistingScript() throws Exception {
        LOGGER.info("renameScriptShouldNotOverwriteExistingScript");
        sieveRepository.putScript(USER, SCRIPT_NAME, SCRIPT_CONTENT);
        sieveRepository.putScript(USER, OTHER_SCRIPT_NAME, OTHER_SCRIPT_CONTENT);
        sieveRepository.renameScript(USER, SCRIPT_NAME, OTHER_SCRIPT_NAME);
    }

    @Test(expected = QuotaNotFoundException.class)
    public void getQuotaShouldThrowIfQuotaNotFound() throws Exception {
        LOGGER.info("getQuotaShouldThrowIfQuotaNotFound");
        sieveRepository.getDefaultQuota();
    }

    @Test
    public void getQuotaShouldWork() throws Exception {
        LOGGER.info("getQuotaShouldWork");
        sieveRepository.setDefaultQuota(DEFAULT_QUOTA);
        assertThat(sieveRepository.getDefaultQuota()).isEqualTo(DEFAULT_QUOTA);
    }

    @Test
    public void getQuotaShouldWorkOnUsers() throws Exception {
        LOGGER.info("getQuotaShouldWorkOnUsers");
        sieveRepository.setQuota(USER, USER_QUOTA);
        assertThat(sieveRepository.getQuota(USER)).isEqualTo(USER_QUOTA);
    }

    @Test
    public void hasQuotaShouldReturnFalseWhenRepositoryDoesNotHaveQuota() throws Exception {
        LOGGER.info("hasQuotaShouldReturnFalseWhenRepositoryDoesNotHaveQuota");
        assertThat(sieveRepository.hasDefaultQuota()).isFalse();
    }

    @Test
    public void hasQuotaShouldReturnTrueWhenRepositoryHaveQuota() throws Exception {
        LOGGER.info("hasQuotaShouldReturnTrueWhenRepositoryHaveQuota");
        sieveRepository.setDefaultQuota(DEFAULT_QUOTA);
        assertThat(sieveRepository.hasDefaultQuota()).isTrue();
    }

    @Test
    public void hasQuotaShouldReturnFalseWhenUserDoesNotHaveQuota() throws Exception {
        LOGGER.info("hasQuotaShouldReturnFalseWhenUserDoesNotHaveQuota");
        assertThat(sieveRepository.hasDefaultQuota()).isFalse();
    }

    @Test
    public void hasQuotaShouldReturnTrueWhenUserHaveQuota() throws Exception {
        LOGGER.info("hasQuotaShouldReturnTrueWhenUserHaveQuota");
        sieveRepository.setQuota(USER, DEFAULT_QUOTA);
        assertThat(sieveRepository.hasQuota(USER)).isTrue();
    }

    @Test
    public void removeQuotaShouldNotThrowIfRepositoryDoesNotHaveQuota() throws Exception {
        LOGGER.info("removeQuotaShouldNotThrowIfRepositoryDoesNotHaveQuota");
        sieveRepository.removeQuota();
    }

    @Test
    public void removeUserQuotaShouldNotThrowWhenAbsent() throws Exception {
        LOGGER.info("removeUserQuotaShouldNotThrowWhenAbsent");
        sieveRepository.removeQuota(USER);
    }

    @Test
    public void removeQuotaShouldWorkOnRepositories() throws Exception {
        LOGGER.info("removeQuotaShouldWorkOnRepositories");
        sieveRepository.setDefaultQuota(DEFAULT_QUOTA);
        sieveRepository.removeQuota();
        assertThat(sieveRepository.hasDefaultQuota()).isFalse();
    }

    @Test
    public void removeQuotaShouldWorkOnUsers() throws Exception {
        LOGGER.info("removeQuotaShouldWorkOnUsers");
        sieveRepository.setQuota(USER, USER_QUOTA);
        sieveRepository.removeQuota(USER);
        assertThat(sieveRepository.hasQuota(USER)).isFalse();
    }

    @Test(expected = QuotaNotFoundException.class)
    public void removeQuotaShouldWorkOnUsersWithGlobalQuota() throws Exception {
        LOGGER.info("removeQuotaShouldWorkOnUsersWithGlobalQuota");
        sieveRepository.setDefaultQuota(DEFAULT_QUOTA);
        sieveRepository.setQuota(USER, USER_QUOTA);
        sieveRepository.removeQuota(USER);
        sieveRepository.getQuota(USER);
    }

    @Test
    public void setQuotaShouldWork() throws Exception {
        LOGGER.info("setQuotaShouldWork");
        sieveRepository.setDefaultQuota(DEFAULT_QUOTA);
        assertThat(sieveRepository.getDefaultQuota()).isEqualTo(DEFAULT_QUOTA);
    }

    @Test
    public void setQuotaShouldWorkOnUsers() throws Exception {
        LOGGER.info("setQuotaShouldWorkOnUsers");
        sieveRepository.setQuota(USER, DEFAULT_QUOTA);
        assertThat(sieveRepository.getQuota(USER)).isEqualTo(DEFAULT_QUOTA);
    }

    protected ScriptContent getScriptContent(InputStream inputStream) throws IOException {
        return new ScriptContent(IOUtils.toString(inputStream, StandardCharsets.UTF_8));
    }

    protected abstract SieveRepository createSieveRepository() throws Exception;

}
