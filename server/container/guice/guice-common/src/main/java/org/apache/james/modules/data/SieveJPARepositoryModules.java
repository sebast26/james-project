package org.apache.james.modules.data;

import org.apache.james.sieve.jpa.JPASieveRepository;
import org.apache.james.sieverepository.api.SieveQuotaRepository;
import org.apache.james.sieverepository.api.SieveRepository;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

public class SieveJPARepositoryModules extends AbstractModule {
    @Override
    protected void configure() {
        bind(JPASieveRepository.class).in(Scopes.SINGLETON);

        bind(SieveRepository.class).to(JPASieveRepository.class);
        bind(SieveQuotaRepository.class).to(JPASieveRepository.class);
    }
}
