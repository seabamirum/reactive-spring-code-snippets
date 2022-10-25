package birdr.life.ws;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketMessage.Type;
import org.springframework.web.reactive.socket.WebSocketSession;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.firebase.messaging.Notification;

import birdr.life.birder.BirderRepo;
import birdr.life.firebase.FirebasePushService;
import birdr.life.message.ChatRepo;
import birdr.life.message.Message;
import birdr.life.message.MessageRepo;
import birdr.life.message.MessageSavedEvent;
import birdr.life.security.UserService;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;

@Component
public class ReactiveWebSocketHandler implements WebSocketHandler, ApplicationListener<MessageSavedEvent>
{	
	Logger logger = LoggerFactory.getLogger(ReactiveWebSocketHandler.class);
	
    private ObjectMapper objectMapper = JsonMapper.builder().serializationInclusion(Include.NON_NULL).build();
    
    private static final long PING_INTERVAL_SEC = 30l;
    private static final long PING_PONG_TIMEOUT_SEC = PING_INTERVAL_SEC*2+1l;
    
    private static Cache<String,String> firebaseTokenCache = Caffeine.newBuilder().expireAfterWrite(Duration.ofHours(1l)).initialCapacity(500).build();
    
    @Autowired
    private FirebasePushService firebasePushService;
    
    @Autowired
    private BirderRepo birderRepo;
    
    @Autowired
    private ChatRepo chatRepo;
	
    @PostConstruct
    private void init()
    {
    	objectMapper.registerModule(new JavaTimeModule());
    }
    
    public static void updateTokenCache(String username,@Nullable String token)
    {
    	String curToken = firebaseTokenCache.getIfPresent(username);
    	if (curToken != null && !curToken.equals(token))
    	{
    		if (token == null)
    			firebaseTokenCache.invalidate(username);
    		else
    			firebaseTokenCache.put(username,token);
    	}
    }
    
	@Autowired
    private MessageRepo messageRepo;	
	
	@Autowired
	private Validator validator;
	
	private static final Multimap<String,Pair<String,Many<WebSocketResponse>>> topicSessionIdMap = Multimaps.synchronizedMultimap(ArrayListMultimap.create());	
	
	/**
	 * @param topic
	 * @param res
	 * @return true if an active websocket session exists for this topic. The event may or may not be emitted successfully
	 */
	public static Mono<Boolean> emitEvent(String topic,WebSocketResponse res)
	{
		Collection<Pair<String, Many<WebSocketResponse>>> col = topicSessionIdMap.get(topic);
		if (col.isEmpty())
			return Mono.empty();
		
		col.stream().forEach(p ->
		{
			res.setMessageRead();
			p.getRight().emitNext(res,Sinks.EmitFailureHandler.FAIL_FAST);
		});
		
		return Mono.just(true);
	}
	
	private Mono<Boolean> emitFirebaseNotif(String toUsername,Message msg)
	{
		logger.debug("Active websocket connection for " + toUsername + " not found, trying firebase notif...");

		Mono<String> tokenMono;
		String cachedToken = firebaseTokenCache.getIfPresent(toUsername);
		if (cachedToken != null)
			tokenMono = Mono.just(cachedToken);
		else
			tokenMono = birderRepo.loadFirebaseToken(toUsername).doOnNext(token -> firebaseTokenCache.put(toUsername,token));
		
		return tokenMono.flatMap(token ->
		{
			return birderRepo.miniProfile(msg.getFromUsername()).flatMap(fromUser -> 
			{
				String title = "Convo with " + fromUser.getFirstName();
				String body = "@" + msg.getFromUsername() + ": " + msg.getText();
				
				Notification notif = Notification.builder().setTitle(title).setBody(body).build();	
				logger.debug("Sending firebase notif to " + toUsername);
				return firebasePushService.send(token,notif).then(Mono.just(true));
			});
		});	
	}
	
	private WebSocketMessage objToWebSocketMsg(WebSocketResponse res,WebSocketSession webSocketSession)
	{
		String text = null;
		try {
			text = objectMapper.writeValueAsString(res);
		} catch (JsonProcessingException e) 
		{
			logger.error("Cannot convert obj to str",e);
			return webSocketSession.textMessage("ERROR");
		}
		
		return webSocketSession.textMessage(text);
	}		
	
	private Mono<Boolean> emitEventOrFirebase(String username,WebSocketResponse res) 
	{
		return emitEvent(username,res)
			   .switchIfEmpty(emitFirebaseNotif(username,(Message)res.getData()));
	}
	
	@Override
	public void onApplicationEvent(MessageSavedEvent event) 
	{
		String destId = event.getDestId();
		boolean isChat = destId.startsWith("CHAT");
		String command = isChat ? "chat-message" : "send-message";
		Message msg = (Message)event.getSource();
		
		WebSocketResponse sucRes = new WebSocketResponse(HttpStatus.CREATED.value(),command);
		sucRes.setMessageId(msg.getMessageId());
		sucRes.setDestId(destId);
		
		emitEvent(msg.getFromUsername(),sucRes);		
		sucRes.setData(msg);
		
		if (isChat)
			chatRepo.members(destId).flatMap(username -> emitEventOrFirebase(username,sucRes).onErrorComplete()).blockLast();
		else
			emitEventOrFirebase(destId,sucRes).block();
	}
	
	private boolean validateMessage(WebSocketCommand wsc,String fromUsername)
	{
		Message message = wsc.getMessage();
		Set<ConstraintViolation<Message>> errors = validator.validate(message);
		if (!errors.isEmpty())
		{
			List<String> errorList = errors.stream().map(cv -> cv.getMessage()).collect(Collectors.toList());
			WebSocketResponse errRes = new WebSocketResponse(HttpStatus.BAD_REQUEST.value(),wsc.getCommand());
			errRes.setFrame(wsc);
			errRes.setErrors(errorList);
			emitEvent(fromUsername,errRes);
			return false;
		}
		
		return true;
	}

    @Override
    public Mono<Void> handle(WebSocketSession webSocketSession) 
    {   
    	
    	return
			UserService.getLoggedInUsername().flatMap(username -> 
			{
				logger.debug("Creating WebSocket Session " + webSocketSession.getId() + " for " + username);
				
				StopWatch pingPongTimer = StopWatch.create();

				String sessionId = webSocketSession.getId();
				Many<WebSocketResponse> generator = Sinks.many().unicast().onBackpressureBuffer();	
				topicSessionIdMap.put(username,Pair.of(sessionId,generator));
				
				Flux<WebSocketMessage> eventFlux = generator.asFlux().map(res -> objToWebSocketMsg(res,webSocketSession));				
				Flux<WebSocketMessage> pingFlux = Flux.interval(Duration.ofSeconds(PING_INTERVAL_SEC)).map(l ->
				{
					if (pingPongTimer.getTime(TimeUnit.SECONDS) > PING_PONG_TIMEOUT_SEC)
					{
						logger.debug("PONG timeout exceeded");
						throw new RuntimeException("PONG timeout exceeded");
					}
					
					if (!pingPongTimer.isStarted())
						pingPongTimer.start();
					
					DataBuffer timeAlive = webSocketSession.bufferFactory().wrap(new byte[] {l.byteValue()});
					return new WebSocketMessage(WebSocketMessage.Type.PING,timeAlive);
				});
				
				Flux<WebSocketMessage> sendFlux = eventFlux.mergeWith(pingFlux);
				
				Flux<Object> rcvFlux = webSocketSession.receive().flatMap(wsm ->
				{
					Type type = wsm.getType();
					
					if (type == Type.PONG)
						pingPongTimer.reset();
					
					if (type != Type.TEXT)
						return Mono.empty();
					
					WebSocketCommand wsc = null;
					try {
						wsc = objectMapper.readValue(wsm.getPayload().asInputStream(),WebSocketCommand.class);
					} 
					catch (Exception e)
					{
						return Mono.error(e);
					}
					
					if (!validateMessage(wsc,username))
						return Mono.empty();
					
					if ("send-message".equals(wsc.getCommand()))
					{
						String toUsername = wsc.getUsername();
						return messageRepo.save(toUsername,wsc.getMessage());
					}
					else if ("chat-message".equals(wsc.getCommand()))
					{
						String chatId = wsc.getChatId();
						Message message = wsc.getMessage();						
						return chatRepo.sendChatMsg(chatId,message);
					}
					
					return Mono.empty();
				});
		    	
		        return webSocketSession.send(sendFlux).and(rcvFlux).doFinally(st -> 
		        {
		        	logger.debug("Removing " + sessionId + " from registry");
		        	synchronized (topicSessionIdMap) {topicSessionIdMap.get(username).removeIf(p -> sessionId.equals(p.getLeft()));};		        	
		        });
			}).switchIfEmpty(webSocketSession.close(CloseStatus.POLICY_VIOLATION));
    }
	
};
