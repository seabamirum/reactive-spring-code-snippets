import java.io.IOException;
import java.util.List;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.Message;
import com.google.firebase.messaging.Notification;

import reactor.core.publisher.Mono;

@Service
public class FirebasePushService 
{
	Logger logger = LoggerFactory.getLogger(FirebasePushService.class);	
	
	private FirebaseMessaging notifSender;
	
    @PostConstruct
    private void init()
    {
    	GoogleCredentials googleCredentials = null;
    	try
    	{
    		googleCredentials = GoogleCredentials
				.fromStream(new ClassPathResource("firebase_service_key.json").getInputStream())
		        .createScoped(List.of("https://www.googleapis.com/auth/cloud-platform"));
    	} 
    	catch (IOException e) 
		{
			logger.error("Cannot make oauth creds",e);
			return;
		}    	
    	
    	FirebaseOptions options = FirebaseOptions.builder()
    		    .setCredentials(googleCredentials)
    		    .build();

    	FirebaseApp.initializeApp(options);
    	
    	notifSender = FirebaseMessaging.getInstance();
    }   

    public Mono<String> send(String targetToken,Notification notification)
    {
    	Message message = Message.builder()
    		.setNotification(notification)
    	    .putData("username", "test")
    	    .setToken(targetToken)
    	    .build();
    	
    	return Mono.fromCallable(() -> notifSender.send(message));
    }
}
