package org.ameet.config;

import com.jcraft.jsch.ChannelSftp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.file.filters.AcceptOnceFileListFilter;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.sftp.inbound.SftpInboundFileSynchronizer;
import org.springframework.integration.sftp.inbound.SftpInboundFileSynchronizingMessageSource;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.support.PeriodicTrigger;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by ameet.chaubal on 8/17/2017.
 */
@Configuration
public class PollerFlow {
    private static final Logger LOGGER = LoggerFactory.getLogger(PollerFlow.class);
    @Value("${poller.sftp.host}")
    private String sftpHost;
    @Value("${poller.sftp.port}")
    private int sftpPort;
    @Value("${poller.sftp.user}")
    private String sftpUser;
    @Value("${poller.sftp.password}")
    private String sftpPass;
    @Value("${poller.sftp.remote.dir}")
    private String sftpRemoteDir;
    @Value("${poller.sftp.local.dir}")
    private String sftpLocalDir;
    private ExecutorService service = Executors.newFixedThreadPool(5);

    @Bean
    public AsyncTaskExecutor pollerService() {
        SimpleAsyncTaskExecutor task = new SimpleAsyncTaskExecutor();
        task.setThreadNamePrefix("Poller_");
        return task;
    }

    @Bean
    public SessionFactory<ChannelSftp.LsEntry> sftpSessionFactory() {
        DefaultSftpSessionFactory factory = new DefaultSftpSessionFactory(true);
        factory.setHost(sftpHost);
        factory.setPort(sftpPort);
        factory.setUser(sftpUser);
        factory.setPassword(sftpPass);
        factory.setAllowUnknownKeys(true);
        return new CachingSessionFactory<ChannelSftp.LsEntry>(factory);
    }

    @Bean
    public SftpInboundFileSynchronizer sftpInboundFileSynchronizer() {
        SftpInboundFileSynchronizer fileSynchronizer = new SftpInboundFileSynchronizer(sftpSessionFactory());
        fileSynchronizer.setDeleteRemoteFiles(false);
        fileSynchronizer.setRemoteDirectory(sftpRemoteDir);
        return fileSynchronizer;
    }

    @Bean
    public MessageChannel queueChannel() {
        return new QueueChannel(5);
    }

    @Bean(name = PollerMetadata.DEFAULT_POLLER)
    public PollerMetadata defaultPoller() {
        PollerMetadata pollerMetadata = new PollerMetadata();
        pollerMetadata.setTrigger(new PeriodicTrigger(5000L));
        return pollerMetadata;
    }

    @Bean
    @InboundChannelAdapter(channel = "queueChannel")
    public MessageSource<File> sftpMessageSource() {
        SftpInboundFileSynchronizingMessageSource source =
                new SftpInboundFileSynchronizingMessageSource(sftpInboundFileSynchronizer());
        source.setLocalDirectory(new File(sftpLocalDir));
        source.setAutoCreateLocalDirectory(true);
        source.setLocalFilter(new AcceptOnceFileListFilter<File>());

        return source;
    }

    @ServiceActivator(inputChannel = "queueChannel")
    public void handler(Message<?> message) {
        Observable.just(message).
                subscribeOn(Schedulers.computation()).
                subscribe(message1 -> {
                    LOGGER.debug("In HANDLE.............{}", message.getPayload());
                    try {
                        Thread.sleep(5000L);
                    } catch (InterruptedException e) {
                        LOGGER.error("Err: sleeping", e);
                    }
                });
    }
}
