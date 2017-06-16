import java.io.IOException;
import org.jruby.Ruby;
import org.jruby.runtime.load.BasicLibraryService;
import org.logstash.persistedqueue.JRubyPqLocalExtLibrary;

public class JrubyAckedBatchExtService implements BasicLibraryService {
    public boolean basicLoad(final Ruby runtime)
        throws IOException {
        new JRubyPqLocalExtLibrary().load(runtime, false);
        return true;
    }
}
