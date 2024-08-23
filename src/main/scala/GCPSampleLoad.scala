import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.cloud.pubsub.v1.MessageReceiver
import com.google.cloud.pubsub.v1.Subscriber
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.PubsubMessage
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import scala.util.{Try,Success,Failure}

object GCPSampleLoad extends App{

    val projectId: String = ""
    val subscriptionId: String = ""

    subscribeAsyncFunc(projectId, subscriptionId)

    def subscribeAsyncFunc(projectId: String, subscriptionId: String): Unit = {
        val subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId)

        // Instantiate an asynchronous message receiver.
        val receiver:MessageReceiver = (message: PubsubMessage, consumer: AckReplyConsumer) => {
            // Handle incoming message, then ack the received message.
            println("Id: " + message.getMessageId)
            println("Data: " + message.getData.toStringUtf8)
            Try{
                //data processing
                println("data processing is ongoing ...")
            } match {
                case Success(_) => {
                    println("data is processed successfully.")
                    consumer.ack()
                }
                case Failure(e) => {
                    println(e)
                    consumer.nack()
                }
            }
        }

        var subscriber: Subscriber = null
        try {
            subscriber = Subscriber.newBuilder(subscriptionName, receiver).build
            // Start the subscriber.
            subscriber.startAsync.awaitRunning
            System.out.printf("Listening for messages on %s:\n", subscriptionName.toString)
            // Allow the subscriber to run for 5 seconds unless an unrecoverable error occurs.
            subscriber.awaitTerminated(5, TimeUnit.SECONDS)
        } catch {
            case timeoutException: TimeoutException =>
                // Shut down the subscriber after 5s. Stop receiving messages.
                subscriber.stopAsync
        }
    }
}