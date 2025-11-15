package org.bit.core.net.message.pbft;

import org.bit.core.capsule.PbftSignCapsule;
import org.bit.core.net.message.MessageTypes;
import org.bit.core.net.message.BitMessage;
import org.bit.protos.Protocol.PBFTCommitResult;

public class PbftCommitMessage extends BitMessage {

  private PbftSignCapsule pbftSignCapsule;

  public PbftCommitMessage(byte[] data) {
    super(data);
    this.type = MessageTypes.PBFT_COMMIT_MSG.asByte();
    this.pbftSignCapsule = new PbftSignCapsule(data);
  }

  public PbftCommitMessage(PbftSignCapsule pbftSignCapsule) {
    data = pbftSignCapsule.getData();
    this.type = MessageTypes.PBFT_COMMIT_MSG.asByte();
    this.pbftSignCapsule = pbftSignCapsule;
  }

  public PBFTCommitResult getPBFTCommitResult() {
    return getPbftSignCapsule().getPbftCommitResult();
  }

  public PbftSignCapsule getPbftSignCapsule() {
    return pbftSignCapsule;
  }

  @Override
  public Class<?> getAnswerMessage() {
    return null;
  }
  
}
