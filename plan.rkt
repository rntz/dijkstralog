#lang racket

(require racket/set)

(define-syntax-rule (todo) (error "todo"))
(define-syntax-rule (assert! test)
  (unless test
    (error "ASSERTION FAILURE:" 'test)))

(define-syntax-rule (for*/append (for-clause ...) body ... expr)
  (for*/list (for-clause ... #:do [body ...] [x expr]) x))

(define term? any/c)
(define var? symbol?)
(define atom? (cons/c symbol? (listof term?)))
(define conjunctive-query? (listof atom?))
(define query-plan? any/c) ;; TODO: contract for query plans
(define decls? any/c)      ;; TODO: contract for decls
(define var-order? (listof var?))

(define atom-pred car)
(define atom-args cdr)

(define (deduplicate seq)
  (define visited (mutable-set))
  (for/list ([x seq] #:when (not (set-member? visited x)))
    (set-add! visited x)
    x))

;; A notion of subsequence that allows terms to be duplicated in xs. We assume
;; terms are not duplicated in ys but do not check this. We can use a term from
;; ys multiple times. eg:
;;
;; (subsequence '(a c) '(a b c))   ==> (0 2)
;; (subsequence '(b b) '(a b c))   ==> (1 1)
;; (subsequence '(b a) '(a b c))   ==> #f
;; (subsequence '(a b a) '(a b c)) ==> #f
;; (subsequence '(a b b) '(a b c)) ==> (0 1 1)
(define (subsequence xs ys #:by [equal equal?])
  (let loop ([acc '()] [i 0] [xs xs] [ys ys])
    (match* (xs ys)
      [('() _) (reverse acc)]
      [(_ '()) #f]
      [(xs ys)
       (if (equal (car xs) (car ys))
           (loop (cons i acc) i       (cdr xs) ys)
           (loop acc          (+ 1 i) xs       (cdr ys)))])))

;; Heuristically picks a variable order for a conjunctive query.
;; Puts variables which are used in more atoms first.
;; After that, vaguely tries to not reorder variables.
(define/contract (heuristic-var-order query)
  (-> conjunctive-query? var-order?)
  (define counts (make-hash))
  (define atom-orders
    (for/list ([atom query])
      (deduplicate (filter var? (atom-args atom)))))
  (for* ([order atom-orders] [x order])
    (hash-update! counts x add1 0))
  (define levels (make-hash))
  (for ([(x n) counts])
    (hash-update! levels n (curry cons x) '()))
  (for*/append ([n (sort (hash-keys levels) >)])
    (define xs (hash-ref levels n))
    ;; (inversions x) = how many times do other variables in this level appear
    ;; before x? This is an imperfect heuristic for avoiding variable
    ;; reordering.
    (define (inversions x)
      (for*/sum ([y xs]
                 #:when (not (equal? x y))
                 [order atom-orders]
                 #:do [(define after-y (member y order))]
                 #:when (and after-y (member x after-y)))
        1))
    (sort xs < #:key inversions #:cache-keys? #t)))

(define/contract (compile-auto query)
  (-> conjunctive-query? (values var-order? decls? query-plan?))
  (define var-order (heuristic-var-order query))
  (define-values (decls query-plan) (compile var-order query))
  (values var-order decls query-plan))

;; Compiles a single atom to a query plan, deriving the variable order from the
;; atom. Doesn't need any additional decls. Used for planning sorts.
(define/contract (compile-atom atom)
  (-> atom? (values var-order? query-plan?))
  (define vars-seen (mutable-set))
  (define var-order (deduplicate (filter var? (atom-args atom))))
  (define-values (decls plan) (compile var-order `(,atom)))
  (assert! (null? decls))
  (values var-order plan))

;; (compile var-order query) --> (values query-plan decls)
;; decls: a list of indices that we need for this query
(define/contract (compile var-order query)
  (-> var-order? conjunctive-query? (values decls? query-plan?))
  (assert! (= (length var-order) (set-count (list->set var-order))))
  (assert! (for*/and ([atom query]
                      [x (atom-args atom)]
                      #:when (var? x))
             (member x var-order)))

  ;; NOTE/TODO: we could get rid of constants and duplicated variables up front
  ;; (as operations on trie iterators), instead of pushing that work through.
  ;; That would avoid the need to handle it separately in each of
  ;; sorting-and-projection and conjunctive-querying.
  (define decls '())
  (define query-sorted
    (for/list ([atom query])
      ;; Only the first occurrence of a variable matters; the others can be
      ;; lookups; so we deduplicate.
      (define atom-var-order (deduplicate (filter var? (atom-args atom))))
      (if (subsequence atom-var-order var-order) atom
          ;; Otherwise, need to re-sort!
          (let* ([new-pred (gensym (atom-pred atom))]
                 [new-vars (filter (lambda (x) (member x atom-var-order)) var-order)]
                 [new-atom `(,new-pred ,@new-vars)]
                 ;; TODO: re-use indices via hash-consing re-sortings
                 [sort-decl `(sort ,new-atom ,atom)])
            (set! decls (cons sort-decl decls))
            new-atom))))

  ;; mutable local vars for planning query
  (define vars-seen (mutable-set))
  (define reverse-query-plan '())
  (define iter-states ;; tracks each trie iterator's progress into its relation
    (for/list ([atom query-sorted])
      (mcons (atom-pred atom) (atom-args atom))))

  (define (plan-push! x) (set! reverse-query-plan (cons x reverse-query-plan)))
  (define (lookups!)
    (for ([iter-state iter-states])
      (define-values (lookups leftover)
        (splitf-at (mcdr iter-state)
                   (λ (x) (or (not (var? x)) (set-member? vars-seen x)))))
      (set-mcdr! iter-state leftover)
      (for ([x lookups])
        (plan-push! `(lookup ,(mcar iter-state) ,x)))))

  ;; Plan the query.
  (lookups!)
  (for ([var var-order])
    (set-add! vars-seen var)
    ;; Find the relations which touch this variable and inner join them.
    (define join-iters
      (for/list ([iter-state iter-states]
                 #:do [(match-define (mcons iter iter-args) iter-state)]
                 #:when (not (null? iter-args))
                 #:when (equal? var (car iter-args)))
        (set-mcdr! iter-state (cdr iter-args))
        iter))
    (plan-push! `(,var <- ,@join-iters))
    (lookups!))

  (values decls (reverse reverse-query-plan)))

(module+ tests
  ;; triangle query with a lookup in T.
  (compile '(a b c) '((R a b) (S b c) (T a 17 c)))
  ;; make sure it generates a sort-and-project step for T.
  (compile '(a b c) '((R a b) (S b c) (T c 17 a)))
  ;; make sure it looks up the second b in S.
  (compile '(a b c) '((R a b) (S b b) (T a 17 c)))
  ;; make sure it looks up the second b in S.
  (compile '(a b c) '((R a b) (S b c b)))

  ;; make sure we can compile a sort body.
  (compile-atom '(T c 17 a))

  ;; make sure heuristic-var-order can handle constants and repeated vars
  (heuristic-var-order '((T c 17 a)))
  (heuristic-var-order '((T c a c)))
  )
