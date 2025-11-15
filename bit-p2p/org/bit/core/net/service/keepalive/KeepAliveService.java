package org.bit.core.net.service.keepalive;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.bit.core.net.message.MessageTypes;
import org.bit.core.net.message.BitMessage;
import org.bit.core.net.message.keepalive.PongMessage;
import org.bit.core.net.peer.PeerConnection;

@Slf4j(topic = "net")
@Component
public class KeepAliveService {

  public void processMessage(PeerConnection peer, BitMessage message) {
    if (message.getType().equals(MessageTypes.P2P_PING)) {
      peer.sendMessage(new PongMessage());
    }
  }
}
