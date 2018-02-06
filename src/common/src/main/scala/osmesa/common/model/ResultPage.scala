package osmesa.common.model

import io.circe.generic.JsonCodec
import io.circe._


@JsonCodec
case class ResultPage[RESULT, START, MAX](
  results: List[RESULT],
  nextStart: START,
  maxResults: MAX
)

