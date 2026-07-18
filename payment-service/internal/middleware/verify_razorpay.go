package middleware

import( 
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"github.com/gin-gonic/gin"
)

func VerifyRazorpay(secret string) gin.HandlerFunc {
	//this closure remebes the secret forever
	return func(c *gin.Context) {
		//read the sign for req header 
		signature := c.GetHeader("X-Razorpay-Signature")
		if signature == ""{
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "missiong razorpay signature",
			})
			return
		}	

		//razorpay computs the sign suing the req body which is a stream of bytes
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"error": "invalid request body",
			})
			return
		}
	
		h := hmac.New(sha256.New, []byte(secret))
		//write never actully returns an error but the function has a return type of int,err
		_, _  = h.Write(body)
		computed := h.Sum(nil) 
		//razorpay sends hexadecimal so 
		computedHex := hex.EncodeToString(computed)

		if !hmac.Equal(
			[]byte(signature),
			[]byte(computedHex),
		) {
			c.AbortWithStatusJSON(
				http.StatusUnauthorized,
				gin.H{
					"error": "invalid razorpay signature",
				},
			)
			return 
		}
		
		// Restore the request body so downstream handlers can read it.
c.Request.Body = io.NopCloser(bytes.NewBuffer(body))


		c.Next()
	}
}