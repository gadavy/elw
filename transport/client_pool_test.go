package transport

import (
	"testing"
)

func BenchmarkClusterPool_NextLive(b *testing.B) {
	b.StopTimer()

	pool, err := NewClientsPool(
		[]string{"http://127.0.0.1:9200",
			"http://127.0.0.1:9201",
			"http://127.0.0.1:9202",
			"http://127.0.0.1:9203",
			"http://127.0.0.1:9204",
			"http://127.0.0.1:9205",
			"http://127.0.0.1:9206",
			"http://127.0.0.1:9207",
			"http://127.0.0.1:9208",
			"http://127.0.0.1:9209",
		},
		"test-user-agent",
	)
	if err != nil {
		b.Fatal(err)
	}

	for {
		client, err := pool.NextDead()
		if err != nil {
			break
		}

		pool.OnSuccess(client)
	}

	b.StartTimer()

	b.Run("Single", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			client, err := pool.NextLive()
			if err != nil {
				b.Fatal(err)
			}

			pool.OnSuccess(client)
		}
	})

	b.Run("Parallel (10)", func(b *testing.B) {
		b.SetParallelism(10)

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				client, err := pool.NextLive()
				if err != nil {
					b.Fatal(err)
				}

				pool.OnSuccess(client)
			}
		})
	})

	b.Run("Parallel (100)", func(b *testing.B) {
		b.SetParallelism(100)

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				client, err := pool.NextLive()
				if err != nil {
					b.Fatal(err)
				}

				pool.OnSuccess(client)
			}
		})
	})

	b.Run("Parallel (1000)", func(b *testing.B) {
		b.SetParallelism(1000)

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				client, err := pool.NextLive()
				if err != nil {
					b.Fatal(err)
				}

				pool.OnSuccess(client)
			}
		})
	})
}
